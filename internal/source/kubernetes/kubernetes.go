// Package kubernetes discovers workloads, their network addresses and their configuration in a
// Kubernetes cluster. It only lists objects: it never reads Secrets and never changes anything.
package kubernetes

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
)

// Source is a Kubernetes cluster.
type Source struct {
	name     string
	settings config.KubernetesConfig
	client   kubernetes.Interface
}

// New creates a source from the kubeconfig. It does not connect: the first request does.
func New(name string, settings config.KubernetesConfig) (*Source, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = settings.Kubeconfig
	overrides := &clientcmd.ConfigOverrides{CurrentContext: settings.Context}
	restConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("kubernetes source %s: %w", name, err)
	}
	restConfig.UserAgent = "xdq"
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("kubernetes source %s: %w", name, err)
	}

	return newSource(name, settings, client), nil
}

// newSource creates a source on top of a ready client. Tests pass a fake clientset.
func newSource(name string, settings config.KubernetesConfig, client kubernetes.Interface) *Source {
	return &Source{name: name, settings: settings, client: client}
}

// Name returns the name of the source in the config.
func (source *Source) Name() string {
	return source.name
}

// Type returns config.TypeKubernetes.
func (source *Source) Type() string {
	return config.TypeKubernetes
}

// Discover lists the objects of the configured namespaces. Any failed request fails the whole
// discovery.
func (source *Source) Discover(ctx context.Context) (graph.Snapshot, error) {
	state, err := source.fetch(ctx)
	if err != nil {
		return graph.Snapshot{}, fmt.Errorf("kubernetes source %s: %w", source.name, err)
	}

	return buildSnapshot(source.name, source.settings, state), nil
}

// Close does nothing: the client keeps no connections that need closing.
func (source *Source) Close() error {
	return nil
}
