package replay

import (
	"crypto/sha256"
	"os"
	"path/filepath"
)

type outputIDSet map[[sha256.Size]byte]struct{}

func newOutputIDSet(expected uint64) outputIDSet {
	return make(outputIDSet, int(expected))
}

func (set outputIDSet) Remember(id [sha256.Size]byte) bool {
	if _, duplicate := set[id]; duplicate {
		return true
	}
	set[id] = struct{}{}
	return false
}

type atomicOutput struct {
	targetPath    string
	temporaryPath string
	file          *os.File
}

func newAtomicOutput(targetPath string) (*atomicOutput, error) {
	output := &atomicOutput{targetPath: targetPath}
	if targetPath == "" {
		return output, nil
	}
	directory := filepath.Dir(targetPath)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	temporary, err := os.CreateTemp(directory, ".partial-*")
	if err != nil {
		return nil, err
	}
	output.file = temporary
	output.temporaryPath = temporary.Name()
	return output, nil
}

func (output *atomicOutput) Write(data []byte) error {
	if output.file == nil {
		return nil
	}
	_, err := output.file.Write(data)
	return err
}

func (output *atomicOutput) Commit() error {
	if output.file == nil {
		return nil
	}
	if err := output.file.Sync(); err != nil {
		return err
	}
	if err := output.file.Close(); err != nil {
		return err
	}
	output.file = nil
	if err := os.Rename(output.temporaryPath, output.targetPath); err != nil {
		return err
	}
	output.temporaryPath = ""
	return nil
}

func (output *atomicOutput) Abort() {
	if output.file != nil {
		_ = output.file.Close()
		output.file = nil
	}
	if output.temporaryPath != "" {
		_ = os.Remove(output.temporaryPath)
		output.temporaryPath = ""
	}
}
