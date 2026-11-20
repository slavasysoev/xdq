package kubernetes

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
)

// Owner kinds that are followed one more step up: a Deployment owns ReplicaSets, a CronJob owns Jobs.
const (
	kindReplicaSet = "ReplicaSet"
	kindJob        = "Job"
)

// workload is a workload node with its newest pod. The newest pod decides the environment and
// the configuration: during a rollout the old pods still run the old configuration.
type workload struct {
	node   graph.Node
	newest corev1.Pod
}

// buildSnapshot turns the listed objects into workload nodes, identities and configuration values.
// Finished pods (Succeeded, Failed) are skipped: they run nothing, and their IPs may be reused.
// The output is sorted, so the same cluster state always gives the same snapshot.
func buildSnapshot(sourceName string, settings config.KubernetesConfig, cluster clusterState) graph.Snapshot {
	snapshot := graph.NewSnapshot()
	workloads := make(map[graph.NodeID]workload)
	for _, pod := range cluster.pods {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		kind, name := topOwner(pod, cluster.owners)
		node := graph.NewWorkload(sourceName, pod.Namespace, kind, name)
		snapshot.Identities = append(snapshot.Identities, podIdentities(pod, node.ID)...)
		current, seen := workloads[node.ID]
		if !seen || isNewer(pod, current.newest) {
			workloads[node.ID] = workload{node: node, newest: pod}
		}
		snapshot.Observations.Workloads[node.ID] = addPod(snapshot.Observations.Workloads[node.ID], pod)
	}
	for _, id := range slices.Sorted(maps.Keys(workloads)) {
		found := workloads[id]
		found.node.Environment = environment(settings, found.newest)
		snapshot.Nodes = append(snapshot.Nodes, found.node)
		snapshot.ConfigValues = append(snapshot.ConfigValues, configValues(id, found.newest, cluster.configMaps)...)
		slices.SortFunc(snapshot.Observations.Workloads[id].NotReady, func(left, right graph.PodProblem) int {
			return cmp.Compare(left.Pod, right.Pod)
		})
	}
	snapshot.Sort()

	return snapshot
}

// Reasons of a pod that is not ready, besides the waiting reason of a container.
const (
	reasonUnschedulable = "Unschedulable"
	reasonNotReady      = "NotReady"
)

// addPod counts the pod in the observation of its workload. A pod that is not ready is listed
// with its reason.
func addPod(observation graph.WorkloadObservation, pod corev1.Pod) graph.WorkloadObservation {
	observation.Pods++
	if isReady(pod) {
		observation.ReadyPods++

		return observation
	}
	var restarts int32
	for _, status := range pod.Status.ContainerStatuses {
		restarts += status.RestartCount
	}
	observation.NotReady = append(observation.NotReady, graph.PodProblem{
		Pod: pod.Name, Reason: notReadyReason(pod), Restarts: restarts,
	})

	return observation
}

func isReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

// notReadyReason returns the waiting reason of the first container that waits, Unschedulable
// for a pod that is not scheduled, otherwise NotReady (a running pod failing its readiness probe).
func notReadyReason(pod corev1.Pod) string {
	for _, status := range slices.Concat(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses) {
		if status.State.Waiting != nil && status.State.Waiting.Reason != "" {
			return status.State.Waiting.Reason
		}
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse {
			return reasonUnschedulable
		}
	}

	return reasonNotReady
}

// topOwner returns the kind and name of the object that ultimately runs the pod. A pod without
// a controller is its own workload. Kinds other than the built-in ones (an Argo Rollout, an
// operator's custom resource) are kept as they are, lowercased.
func topOwner(pod corev1.Pod, owners map[string]*metav1.OwnerReference) (graph.WorkloadKind, string) {
	owner := metav1.GetControllerOf(&pod)
	if owner == nil {
		return graph.WorkloadPod, pod.Name
	}
	if owner.Kind == kindReplicaSet || owner.Kind == kindJob {
		parent := owners[objectKey(pod.Namespace, owner.Kind, owner.Name)]
		if parent != nil {
			owner = parent
		}
	}

	return graph.WorkloadKind(strings.ToLower(owner.Kind)), owner.Name
}

// podIdentities maps every IP of a running pod to its workload. Pods on the host network share
// the node's IP with other pods, so their IP says nothing about the workload.
func podIdentities(pod corev1.Pod, workloadID graph.NodeID) []graph.Identity {
	if pod.Spec.HostNetwork || pod.Status.Phase != corev1.PodRunning {
		return nil
	}
	var identities []graph.Identity
	for _, address := range pod.Status.PodIPs {
		identities = append(identities, graph.Identity{Address: address.IP, Workload: workloadID})
	}

	return identities
}

// isNewer orders pods by creation time. Pods created in the same second are ordered by name,
// so the choice does not depend on the order the API returned them in.
func isNewer(pod, other corev1.Pod) bool {
	if !pod.CreationTimestamp.Equal(&other.CreationTimestamp) {
		return other.CreationTimestamp.Before(&pod.CreationTimestamp)
	}

	return pod.Name > other.Name
}

// environment returns the namespace or the value of the configured label. A pod without the
// label has no environment.
func environment(settings config.KubernetesConfig, pod corev1.Pod) string {
	labelKey, isLabel := settings.EnvironmentLabel()
	if isLabel {
		return pod.Labels[labelKey]
	}

	return pod.Namespace
}

// configValues collects the configuration of the pod that may name topics: env variables (literal
// and from ConfigMaps) and ConfigMaps mounted as volumes. Secrets are never read. References to
// ConfigMaps that do not exist are skipped: the pod would not start without a required one anyway.
func configValues(workloadID graph.NodeID, pod corev1.Pod, configMaps map[string]corev1.ConfigMap) []graph.ConfigValue {
	// A set, because several containers often share the same variables.
	found := make(map[graph.ConfigValue]bool)
	add := func(origin, value string) {
		if value != "" {
			found[graph.ConfigValue{Workload: workloadID, Origin: origin, Value: value}] = true
		}
	}
	configMapData := func(name string) map[string]string {
		return configMaps[pod.Namespace+"/"+name].Data
	}

	for _, container := range slices.Concat(pod.Spec.InitContainers, pod.Spec.Containers) {
		for _, envSource := range container.EnvFrom {
			if envSource.ConfigMapRef == nil {
				continue
			}
			name := envSource.ConfigMapRef.Name
			for key, value := range configMapData(name) {
				add(fmt.Sprintf("env %s%s (configmap %s)", envSource.Prefix, key, name), value)
			}
		}
		for _, variable := range container.Env {
			switch {
			case variable.ValueFrom == nil:
				add("env "+variable.Name, variable.Value)
			case variable.ValueFrom.ConfigMapKeyRef != nil:
				reference := variable.ValueFrom.ConfigMapKeyRef
				add(fmt.Sprintf("env %s (configmap %s/%s)", variable.Name, reference.Name, reference.Key),
					configMapData(reference.Name)[reference.Key])
			}
		}
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.ConfigMap == nil {
			continue
		}
		name := volume.ConfigMap.Name
		for key, value := range configMapData(name) {
			add("configmap "+name+"/"+key, value)
		}
	}

	return slices.Collect(maps.Keys(found))
}
