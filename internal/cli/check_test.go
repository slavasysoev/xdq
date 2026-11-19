package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/slavasysoev/xdq/internal/report"
)

// runCommand runs the command tree with args and returns stdout and the exit code.
func runCommand(t *testing.T, args ...string) (string, int, error) {
	t.Helper()
	root := NewRootCommand("test")
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs(args)
	root.SilenceUsage = true
	root.SilenceErrors = true

	err := root.ExecuteContext(t.Context())
	if err != nil {
		return stdout.String(), ExitCode(err), err
	}

	return stdout.String(), 0, nil
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "xdq.yaml")
	err := os.WriteFile(path, []byte(content), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	return path
}

const unreachableKafka = `
version: 1
sources:
  main:
    kafka:
      brokers: [127.0.0.1:1]
`

func TestCheckFailsBeforeDiscovery(t *testing.T) {
	t.Setenv("XDQ_TEST_DSN", "postgres://localhost:not-a-port/test")
	postgres := writeConfig(t, "version: 1\nsources:\n  test:\n    postgres: {dsn: '${env:XDQ_TEST_DSN}'}\n")
	unknownRule := writeConfig(t, unreachableKafka+"rules:\n  stalled-consumers:\n    severity: off\n")
	withChain := writeConfig(t, unreachableKafka+"chains:\n  order-flow:\n    from:\n      - topic: orders.created\n")
	tests := []struct {
		name      string
		args      []string
		wantError string
	}{
		{name: "missing config", args: []string{"--config", filepath.Join(t.TempDir(), "missing.yaml")}, wantError: "missing.yaml"},
		{name: "unknown output", args: []string{"--output", "yaml"}, wantError: `invalid --output "yaml"`},
		{name: "zero timeout", args: []string{"--timeout", "0s"}, wantError: "invalid --timeout"},
		{name: "invalid postgres dsn", args: []string{"--config", postgres}, wantError: "source test: postgres source test: invalid dsn"},
		{
			name:      "unknown chain",
			args:      []string{"--config", withChain, "--chain", "order-flw"},
			wantError: `invalid --chain "order-flw": configured chains: order-flow`,
		},
		{name: "unknown rule", args: []string{"--config", unknownRule}, wantError: "rules.stalled-consumers: unknown rule"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, code, err := runCommand(t, append([]string{"check"}, test.args...)...)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Errorf("error = %v, want it to contain %q", err, test.wantError)
			}
			if code != exitCodeIncomplete {
				t.Errorf("exit code = %d, want %d", code, exitCodeIncomplete)
			}
		})
	}
}

func TestCheckUnreachableSource(t *testing.T) {
	configPath := writeConfig(t, unreachableKafka)

	t.Run("text", func(t *testing.T) {
		stdout, code, _ := runCommand(t, "check", "--config", configPath, "--timeout", "2s")
		if code != exitCodeIncomplete {
			t.Errorf("exit code = %d, want %d", code, exitCodeIncomplete)
		}
		if !strings.Contains(stdout, "all  UNKNOWN") || !strings.Contains(stdout, "main  kafka  unknown") {
			t.Errorf("stdout does not show the unknown source and chain:\n%s", stdout)
		}
	})

	t.Run("json", func(t *testing.T) {
		stdout, code, _ := runCommand(t, "check", "--config", configPath, "--timeout", "2s", "--output", "json")
		if code != exitCodeIncomplete {
			t.Errorf("exit code = %d, want %d", code, exitCodeIncomplete)
		}
		var result report.Report
		err := json.Unmarshal([]byte(stdout), &result)
		if err != nil {
			t.Fatalf("stdout is not a report: %v\n%s", err, stdout)
		}
		if result.Status != report.StatusUnknown || result.Sources[0].Error == "" {
			t.Errorf("report = %+v, want unknown with the source error", result)
		}
	})
}
