package cli

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/slavasysoev/xdq/internal/report"
)

func TestLockRefusal(t *testing.T) {
	complete := []report.SourceResult{{Name: "main", Status: report.StatusOK}}
	incomplete := []report.SourceResult{{Name: "main", Status: report.StatusUnknown}}
	errorFindings := []report.Finding{{Rule: "stalled-consumer", Severity: report.SeverityError}}
	ignoredFindings := []report.Finding{{Rule: "stalled-consumer", Severity: report.SeverityError, Ignored: true}}
	warningFindings := []report.Finding{{Rule: "lagging-consumer", Severity: report.SeverityWarning}}
	tests := []struct {
		name     string
		result   report.Report
		force    bool
		wantCode int
	}{
		{name: "clean", result: report.New("", complete, nil, nil), wantCode: exitCodeOK},
		{name: "warnings only", result: report.New("", complete, nil, warningFindings), wantCode: exitCodeOK},
		{name: "ignored error finding", result: report.New("", complete, nil, ignoredFindings), wantCode: exitCodeOK},
		{name: "error findings", result: report.New("", complete, nil, errorFindings), wantCode: exitCodeErrors},
		{name: "error findings, forced", result: report.New("", complete, nil, errorFindings), force: true, wantCode: exitCodeOK},
		{name: "incomplete, forced", result: report.New("", incomplete, nil, nil), force: true, wantCode: exitCodeIncomplete},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, reason := lockRefusal(&test.result, test.force)
			if code != test.wantCode {
				t.Errorf("code = %d (%s), want %d", code, reason, test.wantCode)
			}
		})
	}
}

func TestLockIncompleteRun(t *testing.T) {
	configPath := writeConfig(t, unreachableKafka)

	stdout, code, _ := runCommand(t, "lock", "--config", configPath, "--timeout", "2s", "--force")

	if code != exitCodeIncomplete {
		t.Errorf("exit code = %d, want %d", code, exitCodeIncomplete)
	}
	if !strings.Contains(stdout, "xdq.lock not written: a source is unavailable") {
		t.Errorf("stdout does not explain the refusal:\n%s", stdout)
	}
	_, err := os.Stat(filepath.Join(filepath.Dir(configPath), "xdq.lock"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("lockfile exists after a refused lock: %v", err)
	}
}

func TestCheckLockfileErrors(t *testing.T) {
	configPath := writeConfig(t, unreachableKafka)
	standA := filepath.Join(t.TempDir(), "stand-a.lock")
	err := os.WriteFile(standA, []byte("version: 1\nenvironment: stand-a\nchains: {}\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		args      []string
		wantError string
	}{
		{name: "named lockfile missing", args: []string{"--lock", filepath.Join(t.TempDir(), "missing.lock")}, wantError: "read lockfile"},
		{name: "other environment", args: []string{"--lock", standA, "--env", "stand-b"}, wantError: `is for environment "stand-a", not "stand-b"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, code, err := runCommand(t, append([]string{"check", "--config", configPath}, test.args...)...)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Errorf("error = %v, want it to contain %q", err, test.wantError)
			}
			if code != exitCodeIncomplete {
				t.Errorf("exit code = %d, want %d", code, exitCodeIncomplete)
			}
		})
	}
}
