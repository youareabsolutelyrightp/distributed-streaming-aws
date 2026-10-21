package replay

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type progressRecord struct {
	TimestampEpochMillis         int64                       `json:"timestamp_epoch_ms"`
	Phase                        string                      `json:"phase"`
	AcknowledgedSourceRecords    uint64                      `json:"acknowledged_source_records"`
	PhaseAcknowledgedRecords     uint64                      `json:"phase_acknowledged_source_records"`
	FirstAcknowledgedEpochMicros int64                       `json:"first_acknowledged_epoch_us,omitempty"`
	LastAcknowledgedEpochMicros  int64                       `json:"last_acknowledged_epoch_us,omitempty"`
	CurrentRequestedRate         float64                     `json:"current_requested_rate_per_second"`
	TopicPartitions              map[string]PartitionSummary `json:"topic_partitions,omitempty"`
}

type progressRecorder struct {
	file   *os.File
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	err    error
	sample func() progressRecord
}

func startProgressRecorder(path string, sample func() progressRecord) (*progressRecorder, error) {
	if path == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	recorder := &progressRecorder{
		file: file, stop: make(chan struct{}), done: make(chan struct{}), sample: sample,
	}
	go recorder.run()
	return recorder, nil
}

func (recorder *progressRecorder) run() {
	defer close(recorder.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			recorder.write()
		case <-recorder.stop:
			recorder.write()
			return
		}
	}
}

func (recorder *progressRecorder) write() {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.err != nil {
		return
	}
	value := recorder.sample()
	value.TimestampEpochMillis = time.Now().UnixMilli()
	if err := json.NewEncoder(recorder.file).Encode(value); err != nil {
		recorder.err = fmt.Errorf("write producer progress: %w", err)
		return
	}
	if err := recorder.file.Sync(); err != nil {
		recorder.err = fmt.Errorf("sync producer progress: %w", err)
	}
}

func (recorder *progressRecorder) close() error {
	if recorder == nil {
		return nil
	}
	recorder.once.Do(func() { close(recorder.stop) })
	<-recorder.done
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if closeErr := recorder.file.Close(); recorder.err == nil && closeErr != nil {
		recorder.err = closeErr
	}
	return recorder.err
}
