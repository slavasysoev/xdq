package config

import "strings"

// KubernetesConfig is a Kubernetes cluster.
type KubernetesConfig struct {
	// Kubeconfig is the kubeconfig path. Empty means the client-go default.
	Kubeconfig string `yaml:"kubeconfig"`
	// Context is the kubeconfig context. Empty means the current context.
	Context string `yaml:"context"`
	// Namespaces to read. Empty means all namespaces.
	Namespaces []string `yaml:"namespaces"`
	// Environment tells how a workload's environment is derived: "namespace" (default) or "label:<key>".
	Environment string `yaml:"environment"`
}

// EnvironmentNamespace derives a workload's environment from its namespace.
const EnvironmentNamespace = "namespace"

// Type implements SourceSettings.
func (*KubernetesConfig) Type() string {
	return TypeKubernetes
}

func (kubernetes *KubernetesConfig) validate(validation *validation, path string) {
	environment := kubernetes.Environment
	labelKey, isLabel := strings.CutPrefix(environment, "label:")
	valid := environment == "" || environment == EnvironmentNamespace || (isLabel && labelKey != "")
	if !valid {
		validation.fail(path+".environment", "must be %q or \"label:<key>\"", EnvironmentNamespace)
	}
}

func (kubernetes *KubernetesConfig) resolve(_, _ string) error {
	if kubernetes.Environment == "" {
		kubernetes.Environment = EnvironmentNamespace
	}

	return nil
}
