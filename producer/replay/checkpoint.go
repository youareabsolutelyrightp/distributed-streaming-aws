package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Checkpoint struct {
	Version                    int    `json:"version"`
	RunID                      string `json:"run_id"`
	DatasetID                  string `json:"dataset_id"`
	ReplayConfigSHA256         string `json:"replay_config_sha256,omitempty"`
	PreparedChecksum           string `json:"prepared_checksum,omitempty"`
	PartitioningVersion        string `json:"partitioning_version,omitempty"`
	PartitionHashVectorVersion string `json:"partition_hash_test_vector_version,omitempty"`
	InputPartitionCount        int    `json:"input_partition_count,omitempty"`
	LastContiguousAcknowledged uint64 `json:"last_contiguous_acknowledged_source_line"`
	LastAcknowledgedSourceLine uint64 `json:"last_acknowledged_source_line,omitempty"`
	Started                    bool   `json:"started"`
	Completed                  bool   `json:"completed"`
}

func replayConfigDigest(config Config, manifest Manifest) string {
	values := []string{
		manifest.DatasetID, manifest.PreparedChecksum, config.RunID, BenchmarkProfile,
		BenchmarkGradingMode, "fixed-rate", fmt.Sprint(config.Limit),
		config.InputTopic, fmt.Sprint(config.InputPartitions),
		PartitioningVersion, PartitionHashVectorVersion,
		config.ControlTopic, config.DLQTopic,
		config.MaximumEventTime,
	}
	digest := sha256.Sum256([]byte(strings.Join(values, "\n")))
	return hex.EncodeToString(digest[:])
}

func loadCheckpoint(path string) (Checkpoint, error) {
	var checkpoint Checkpoint
	data, err := os.ReadFile(path)
	if err != nil {
		return checkpoint, err
	}
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return checkpoint, fmt.Errorf("decode checkpoint: %w", err)
	}
	if checkpoint.Version != 4 {
		return checkpoint, fmt.Errorf("unsupported checkpoint version %d", checkpoint.Version)
	}
	return checkpoint, nil
}

func saveJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return saveBytesAtomic(path, data)
}

func saveBytesAtomic(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".partial-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
}

type ackTracker struct {
	mu      sync.Mutex
	last    uint64
	pending map[uint64]struct{}
}

func newAckTracker(initial uint64) *ackTracker {
	return &ackTracker{last: initial, pending: make(map[uint64]struct{})}
}

func (tracker *ackTracker) acknowledge(sourceLine uint64) uint64 {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if sourceLine <= tracker.last {
		return tracker.last
	}
	tracker.pending[sourceLine] = struct{}{}
	for {
		next := tracker.last + 1
		_, exists := tracker.pending[next]
		if !exists {
			break
		}
		delete(tracker.pending, next)
		tracker.last = next
	}
	return tracker.last
}

func validateCheckpoint(checkpoint Checkpoint, config Config, manifest Manifest) error {
	if checkpoint.RunID != config.RunID || checkpoint.DatasetID != manifest.DatasetID {
		return errors.New("checkpoint run or dataset identity does not match")
	}
	_, partitions := config.inputTopicAndPartitions()
	if checkpoint.ReplayConfigSHA256 != replayConfigDigest(config, manifest) {
		return errors.New("checkpoint immutable replay configuration does not match")
	}
	if checkpoint.PreparedChecksum != manifest.PreparedChecksum ||
		checkpoint.PartitioningVersion != PartitioningVersion ||
		checkpoint.PartitionHashVectorVersion != PartitionHashVectorVersion ||
		checkpoint.InputPartitionCount != partitions {
		return errors.New("checkpoint dataset or input partitioning contract does not match")
	}
	if checkpoint.Completed {
		return errors.New("checkpoint already records a completed replay")
	}
	if checkpoint.LastContiguousAcknowledged > config.Limit {
		return errors.New("checkpoint is beyond the requested replay limit")
	}
	return nil
}
