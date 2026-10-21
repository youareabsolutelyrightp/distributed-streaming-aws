package replay

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestAtomicOutputDiscardsWhenPathIsEmpty(t *testing.T) {
	output, err := newAtomicOutput("")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Abort()
	if err := output.Write(bytes.Repeat([]byte("x"), 1024*1024)); err != nil {
		t.Fatal(err)
	}
	if output.file != nil || output.temporaryPath != "" {
		t.Fatal("disabled output created retained storage")
	}
	if err := output.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicOutputStreamsAndCommits(t *testing.T) {
	target := filepath.Join(t.TempDir(), "nested", "outputs.jsonl")
	output, err := newAtomicOutput(target)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Abort()
	want := []byte("first\nsecond\n")
	if err := output.Write(want[:6]); err != nil {
		t.Fatal(err)
	}
	if err := output.Write(want[6:]); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target was visible before commit: %v", err)
	}
	if err := output.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("committed output = %q, want %q", got, want)
	}
}

func TestAtomicOutputAbortRemovesPartialFile(t *testing.T) {
	target := filepath.Join(t.TempDir(), "outputs.jsonl")
	output, err := newAtomicOutput(target)
	if err != nil {
		t.Fatal(err)
	}
	temporary := output.temporaryPath
	if err := output.Write([]byte("partial\n")); err != nil {
		t.Fatal(err)
	}
	output.Abort()
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target exists after abort: %v", err)
	}
	if _, err := os.Stat(temporary); !os.IsNotExist(err) {
		t.Fatalf("temporary file exists after abort: %v", err)
	}
}
