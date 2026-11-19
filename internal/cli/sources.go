package cli

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/source"
	"github.com/slavasysoev/xdq/internal/source/kafka"
	"github.com/slavasysoev/xdq/internal/source/kubernetes"
	"github.com/slavasysoev/xdq/internal/source/postgres"
)

// openSources creates a source for every configured one, in name order. On failure, the sources
// opened so far are closed.
//
// This is the only place that knows every source implementation. A new source type is added
// as one more case here.
func openSources(configuration *config.Config) ([]source.Source, error) {
	var opened []source.Source
	for _, name := range slices.Sorted(maps.Keys(configuration.Sources)) {
		sourceConfig := configuration.Sources[name]
		var (
			created source.Source
			err     error
		)
		switch sourceConfig.Type() {
		case config.TypeKafka:
			created, err = kafka.New(name, *sourceConfig.Kafka)
		case config.TypeKubernetes:
			created, err = kubernetes.New(name, *sourceConfig.Kubernetes)
		case config.TypePostgres:
			created, err = postgres.New(name, *sourceConfig.Postgres)
		default:
			err = fmt.Errorf("%s sources are not implemented yet", sourceConfig.Type())
		}
		if err != nil {
			return nil, errors.Join(fmt.Errorf("source %s: %w", name, err), closeSources(opened))
		}
		opened = append(opened, created)
	}

	return opened, nil
}

func closeSources(sources []source.Source) error {
	var errs []error
	for _, opened := range sources {
		err := opened.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("close source %s: %w", opened.Name(), err))
		}
	}

	return errors.Join(errs...)
}
