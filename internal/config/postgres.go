package config

// PostgresConfig is a Postgres server.
type PostgresConfig struct {
	DSN Secret `yaml:"dsn"`
}

// Type implements SourceSettings.
func (*PostgresConfig) Type() string {
	return TypePostgres
}

func (postgres *PostgresConfig) validate(validation *validation, path string) {
	validation.secret(path+".dsn", postgres.DSN)
}

func (postgres *PostgresConfig) resolve(baseDir, path string) error {
	return resolveSecret(baseDir, path+".dsn", &postgres.DSN)
}
