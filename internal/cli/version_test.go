package cli

import (
	"bytes"
	"testing"
)

func TestVersionCommandPrintsVersion(t *testing.T) {
	root := NewRootCommand("v1.2.3")
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetArgs([]string{"version"})

	err := root.Execute()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	want := "xdq v1.2.3\n"
	if output.String() != want {
		t.Errorf("output = %q, want %q", output.String(), want)
	}
}
