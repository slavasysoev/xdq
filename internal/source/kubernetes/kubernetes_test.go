package kubernetes

import (
	"reflect"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/source"
)

const sourceName = "test"

// Compile-time check that Source implements the source contract.
var _ source.Source = (*Source)(nil)

var created = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func controlledBy(kind, name string) []metav1.OwnerReference {
	return []metav1.OwnerReference{{Kind: kind, Name: name, Controller: new(true)}}
}

// pod returns a running pod with one IP, created at the given minute after created.
func pod(namespace, name, address string, minute int, owners []metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{
		Namespace:         namespace,
		Name:              name,
		OwnerReferences:   owners,
		CreationTimestamp: metav1.NewTime(created.Add(time.Duration(minute) * time.Minute)),
		Status:            corev1.PodStatus{Phase: corev1.PodRunning, PodIPs: []corev1.PodIP{{IP: address}}},
	}
}

func replicaSet(namespace, name, deployment string) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		Namespace: namespace, Name: name, OwnerReferences: controlledBy("Deployment", deployment)}
}

func discover(t *testing.T, settings config.KubernetesConfig, objects ...runtime.Object) graph.Snapshot {
	t.Helper()
	if settings.Environment == "" {
		settings.Environment = config.EnvironmentNamespace
	}
	snapshot, err := newSource(sourceName, settings, fake.NewClientset(objects...)).Discover(t.Context())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	return snapshot
}

func TestDiscoverWorkloads(t *testing.T) {
	cronJobPod := pod("stand-a", "cleanup-29000000-x1", "10.0.0.3", 0, controlledBy(kindJob, "cleanup-29000000"))
	finished := pod("stand-a", "cleanup-28000000-x1", "10.0.0.9", 0, controlledBy(kindJob, "cleanup-28000000"))
	finished.Status.Phase = corev1.PodSucceeded
	hostNetwork := pod("stand-a", "agent-x1", "192.168.1.5", 0, controlledBy("DaemonSet", "agent"))
	hostNetwork.Spec.HostNetwork = true

	snapshot := discover(t, config.KubernetesConfig{},
		replicaSet("stand-a", "billing-5d8f", "billing"),
		pod("stand-a", "billing-5d8f-a", "10.0.0.1", 0, controlledBy(kindReplicaSet, "billing-5d8f")),
		pod("stand-b", "billing-7c9a-a", "10.0.1.1", 0, controlledBy(kindReplicaSet, "billing-7c9a")),
		replicaSet("stand-b", "billing-7c9a", "billing"),
		&batchv1.Job{
			Namespace: "stand-a", Name: "cleanup-29000000", OwnerReferences: controlledBy("CronJob", "cleanup")},
		cronJobPod,
		finished,
		hostNetwork,
		pod("stand-a", "debug", "10.0.0.4", 0, nil),
	)

	billingA := graph.WorkloadID(sourceName, "stand-a", graph.WorkloadDeployment, "billing")
	billingB := graph.WorkloadID(sourceName, "stand-b", graph.WorkloadDeployment, "billing")
	cleanup := graph.WorkloadID(sourceName, "stand-a", graph.WorkloadCronJob, "cleanup")
	agent := graph.WorkloadID(sourceName, "stand-a", graph.WorkloadDaemonSet, "agent")
	debug := graph.WorkloadID(sourceName, "stand-a", graph.WorkloadPod, "debug")

	t.Run("nodes", func(t *testing.T) {
		want := []graph.NodeID{cleanup, agent, billingA, debug, billingB}
		if got := nodeIDs(snapshot); !reflect.DeepEqual(got, want) {
			t.Errorf("nodes = %v, want %v", got, want)
		}
		for _, node := range snapshot.Nodes {
			if node.Environment != node.Namespace {
				t.Errorf("%s: environment = %q, want the namespace", node.ID, node.Environment)
			}
		}
	})
	t.Run("identities skip host network and finished pods", func(t *testing.T) {
		want := []graph.Identity{
			{Address: "10.0.0.1", Workload: billingA},
			{Address: "10.0.0.3", Workload: cleanup},
			{Address: "10.0.0.4", Workload: debug},
			{Address: "10.0.1.1", Workload: billingB},
		}
		if !reflect.DeepEqual(snapshot.Identities, want) {
			t.Errorf("identities = %v, want %v", snapshot.Identities, want)
		}
	})
}

func TestDiscoverEnvironmentFromNewestPodLabel(t *testing.T) {
	settings := config.KubernetesConfig{Environment: "label:stand"}
	old := pod("apps", "billing-a", "10.0.0.1", 0, controlledBy("StatefulSet", "billing"))
	old.Labels = map[string]string{"stand": "stand-a"}
	newer := pod("apps", "billing-b", "10.0.0.2", 5, controlledBy("StatefulSet", "billing"))
	newer.Labels = map[string]string{"stand": "stand-b"}
	unlabeled := pod("apps", "debug", "10.0.0.3", 0, nil)

	snapshot := discover(t, settings, newer, old, unlabeled)

	environments := make(map[graph.NodeID]string)
	for _, node := range snapshot.Nodes {
		environments[node.ID] = node.Environment
	}
	want := map[graph.NodeID]string{
		graph.WorkloadID(sourceName, "apps", graph.WorkloadStatefulSet, "billing"): "stand-b",
		graph.WorkloadID(sourceName, "apps", graph.WorkloadPod, "debug"):           "",
	}
	if !reflect.DeepEqual(environments, want) {
		t.Errorf("environments = %v, want %v", environments, want)
	}
}

func TestDiscoverConfigValues(t *testing.T) {
	billing := pod("stand-a", "billing", "10.0.0.1", 0, nil)
	billing.Spec = corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "app",
			Env: []corev1.EnvVar{
				{Name: "KAFKA_OUT_TOPIC", Value: "invoices.created"},
				{Name: "KAFKA_IN_TOPIC", ValueFrom: &corev1.EnvVarSource{
					ConfigMapKeyRef: &corev1.ConfigMapKeySelector{Name: "topics", Key: "in"},
				}},
				{Name: "PASSWORD", ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{Name: "creds", Key: "password"},
				}},
			},
			EnvFrom: []corev1.EnvFromSource{
				{Prefix: "APP_", ConfigMapRef: &corev1.ConfigMapEnvSource{Name: "topics"}},
				{SecretRef: &corev1.SecretEnvSource{Name: "creds"}},
			},
		}},
		Volumes: []corev1.Volume{
			{Name: "config", ConfigMap: &corev1.ConfigMapVolumeSource{
				Name: "application",
			}},
			{Name: "missing", ConfigMap: &corev1.ConfigMapVolumeSource{
				Name: "does-not-exist",
			}},
		},
	}
	// A second container with the same variable: the value is reported once.
	billing.Spec.InitContainers = []corev1.Container{{Name: "init", Env: []corev1.EnvVar{{Name: "KAFKA_OUT_TOPIC", Value: "invoices.created"}}}}
	client := fake.NewClientset(
		billing,
		&corev1.ConfigMap{Namespace: "stand-a", Name: "topics", Data: map[string]string{"in": "orders.created"}},
		&corev1.ConfigMap{
			Namespace: "stand-a", Name: "application",
			Data: map[string]string{"application.yml": "kafka:\n  topic: orders.created\n"},
		},
		&corev1.Secret{Namespace: "stand-a", Name: "creds", Data: map[string][]byte{"password": []byte("s3cret")}},
	)
	settings := config.KubernetesConfig{Environment: config.EnvironmentNamespace}

	snapshot, err := newSource(sourceName, settings, client).Discover(t.Context())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	workload := graph.WorkloadID(sourceName, "stand-a", graph.WorkloadPod, "billing")
	want := []graph.ConfigValue{
		{Workload: workload, Origin: "configmap application/application.yml", Value: "kafka:\n  topic: orders.created\n"},
		{Workload: workload, Origin: "env APP_in (configmap topics)", Value: "orders.created"},
		{Workload: workload, Origin: "env KAFKA_IN_TOPIC (configmap topics/in)", Value: "orders.created"},
		{Workload: workload, Origin: "env KAFKA_OUT_TOPIC", Value: "invoices.created"},
	}
	if !reflect.DeepEqual(snapshot.ConfigValues, want) {
		t.Errorf("config values =\n%v\nwant\n%v", snapshot.ConfigValues, want)
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "secrets" {
			t.Errorf("xdq read secrets: %v", action)
		}
	}
}

func TestDiscoverOnlyConfiguredNamespaces(t *testing.T) {
	snapshot := discover(t, config.KubernetesConfig{Namespaces: []string{"stand-a"}},
		pod("stand-a", "billing", "10.0.0.1", 0, nil),
		pod("stand-b", "billing", "10.0.1.1", 0, nil),
	)

	want := []graph.NodeID{graph.WorkloadID(sourceName, "stand-a", graph.WorkloadPod, "billing")}
	if got := nodeIDs(snapshot); !reflect.DeepEqual(got, want) {
		t.Errorf("nodes = %v, want %v", got, want)
	}
}

func TestDiscoverIsDeterministic(t *testing.T) {
	objects := []runtime.Object{
		pod("stand-a", "billing-a", "10.0.0.1", 0, controlledBy("StatefulSet", "billing")),
		pod("stand-a", "billing-b", "10.0.0.2", 0, controlledBy("StatefulSet", "billing")),
	}
	first := discover(t, config.KubernetesConfig{}, objects...)
	reversed := slices.Clone(objects)
	slices.Reverse(reversed)
	second := discover(t, config.KubernetesConfig{}, reversed...)

	if !reflect.DeepEqual(first, second) {
		t.Error("snapshot depends on the order the API returns objects in")
	}
}

func nodeIDs(snapshot graph.Snapshot) []graph.NodeID {
	var ids []graph.NodeID
	for _, node := range snapshot.Nodes {
		ids = append(ids, node.ID)
	}

	return ids
}
