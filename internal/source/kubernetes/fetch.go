package kubernetes

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// clusterState is what the cluster reported, before it is turned into a snapshot.
// ReplicaSets and Jobs are only needed to find the top-level owner of a pod, so only their
// owners are kept. ConfigMaps hold the configuration that pods reference.
type clusterState struct {
	pods []corev1.Pod
	// owners maps "<namespace>/<kind>/<name>" of a ReplicaSet or Job to its controller.
	owners     map[string]*metav1.OwnerReference
	configMaps map[string]corev1.ConfigMap
}

// fetch lists pods, ReplicaSets, Jobs and ConfigMaps of every configured namespace.
// No namespaces in the config means all namespaces.
func (source *Source) fetch(ctx context.Context) (clusterState, error) {
	namespaces := source.settings.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}
	state := clusterState{
		owners:     make(map[string]*metav1.OwnerReference),
		configMaps: make(map[string]corev1.ConfigMap),
	}
	for _, namespace := range namespaces {
		err := source.fetchNamespace(ctx, namespace, &state)
		if err != nil {
			return clusterState{}, err
		}
	}

	return state, nil
}

func (source *Source) fetchNamespace(ctx context.Context, namespace string, state *clusterState) error {
	options := metav1.ListOptions{}
	pods, err := source.client.CoreV1().Pods(namespace).List(ctx, options)
	if err != nil {
		return err
	}
	state.pods = append(state.pods, pods.Items...)

	replicaSets, err := source.client.AppsV1().ReplicaSets(namespace).List(ctx, options)
	if err != nil {
		return err
	}
	for _, replicaSet := range replicaSets.Items {
		state.owners[objectKey(replicaSet.Namespace, kindReplicaSet, replicaSet.Name)] = metav1.GetControllerOf(&replicaSet)
	}

	jobs, err := source.client.BatchV1().Jobs(namespace).List(ctx, options)
	if err != nil {
		return err
	}
	for _, job := range jobs.Items {
		state.owners[objectKey(job.Namespace, kindJob, job.Name)] = metav1.GetControllerOf(&job)
	}

	configMaps, err := source.client.CoreV1().ConfigMaps(namespace).List(ctx, options)
	if err != nil {
		return err
	}
	for _, configMap := range configMaps.Items {
		state.configMaps[configMap.Namespace+"/"+configMap.Name] = configMap
	}

	return nil
}

func objectKey(namespace, kind, name string) string {
	return namespace + "/" + kind + "/" + name
}
