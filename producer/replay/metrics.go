package replay

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

var latencyBounds = []time.Duration{
	time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond,
	25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond,
	250 * time.Millisecond, 500 * time.Millisecond, time.Second,
}

type LatencyHistogram struct {
	Buckets map[string]uint64 `json:"buckets"`
}

func newLatencyHistogram() LatencyHistogram {
	histogram := LatencyHistogram{Buckets: make(map[string]uint64, len(latencyBounds)+1)}
	for _, bound := range latencyBounds {
		histogram.Buckets[fmt.Sprintf("le_%d_ms", bound.Milliseconds())] = 0
	}
	histogram.Buckets["gt_1000_ms"] = 0
	return histogram
}

func (histogram *LatencyHistogram) observe(duration time.Duration) {
	for _, bound := range latencyBounds {
		if duration <= bound {
			histogram.Buckets[fmt.Sprintf("le_%d_ms", bound.Milliseconds())]++
		}
	}
	if duration > time.Second {
		histogram.Buckets["gt_1000_ms"]++
	}
}

type PartitionSummary struct {
	Acknowledged                 uint64 `json:"acknowledged"`
	LastOffset                   int64  `json:"last_offset"`
	FirstAcknowledgedEpochMicros int64  `json:"first_acknowledged_epoch_us,omitempty"`
	LastAcknowledgedEpochMicros  int64  `json:"last_acknowledged_epoch_us,omitempty"`
}

type Summary struct {
	Version                        int                         `json:"version"`
	RunID                          string                      `json:"run_id"`
	DatasetID                      string                      `json:"dataset_id"`
	Profile                        string                      `json:"profile"`
	GradingMode                    string                      `json:"grading_mode"`
	InputTopic                     string                      `json:"input_topic"`
	ExpectedPartitionCount         int                         `json:"expected_partition_count"`
	PartitioningVersion            string                      `json:"partitioning_version"`
	PartitionHashVectorVersion     string                      `json:"partition_hash_test_vector_version"`
	ExpectedSourceCount            uint64                      `json:"expected_source_count"`
	ResumedFromSourceLine          uint64                      `json:"resumed_from_source_line"`
	LastContiguousAcknowledgedLine uint64                      `json:"last_contiguous_acknowledged_source_line"`
	PrimaryRecordsQueued           uint64                      `json:"primary_records_queued"`
	DataAcknowledged               uint64                      `json:"data_acknowledged"`
	Failed                         uint64                      `json:"failed"`
	EOSAcknowledged                uint64                      `json:"eos_acknowledged"`
	ControlAcknowledged            uint64                      `json:"control_acknowledged"`
	Paused                         bool                        `json:"paused"`
	Completed                      bool                        `json:"completed"`
	RequestedRatePerSecond         float64                     `json:"requested_rate_per_second"`
	ProgressPhase                  string                      `json:"progress_phase"`
	FirstSourceLine                uint64                      `json:"first_source_line,omitempty"`
	LastSourceLine                 uint64                      `json:"last_source_line,omitempty"`
	FirstEventTime                 string                      `json:"first_event_time,omitempty"`
	LastEventTime                  string                      `json:"last_event_time,omitempty"`
	FirstAcknowledgedEpochMicros   int64                       `json:"first_acknowledged_epoch_us,omitempty"`
	LastAcknowledgedEpochMicros    int64                       `json:"last_acknowledged_epoch_us,omitempty"`
	ElapsedSeconds                 float64                     `json:"elapsed_seconds"`
	SourceRecordsPerSecond         float64                     `json:"source_records_per_second"`
	AcknowledgementLatency         LatencyHistogram            `json:"acknowledgement_latency"`
	TopicPartitions                map[string]PartitionSummary `json:"topic_partitions"`
	Errors                         []string                    `json:"errors,omitempty"`
}

type delivery struct {
	sourceLine     uint64
	sentAt         time.Time
	topic          string
	partition      int32
	offset         int64
	err            error
	acknowledgedAt time.Time
}

type deliveryCollector struct {
	mu              sync.Mutex
	summary         *Summary
	tracker         *ackTracker
	checkpoint      *Checkpoint
	checkpointPath  string
	checkpointEvery uint64
	lastSaved       uint64
	err             error
}

func (collector *deliveryCollector) collect(input <-chan delivery, done chan<- struct{}) {
	defer close(done)
	for result := range input {
		collector.mu.Lock()
		if result.err != nil {
			collector.summary.Failed++
			collector.summary.Errors = append(collector.summary.Errors, result.err.Error())
			collector.mu.Unlock()
			continue
		}
		collector.summary.AcknowledgementLatency.observe(time.Since(result.sentAt))
		acknowledgedAt := result.acknowledgedAt.UnixMicro()
		if collector.summary.FirstAcknowledgedEpochMicros == 0 {
			collector.summary.FirstAcknowledgedEpochMicros = acknowledgedAt
		}
		collector.summary.LastAcknowledgedEpochMicros = acknowledgedAt
		collector.summary.DataAcknowledged++
		partitionKey := fmt.Sprintf("%s/%d", result.topic, result.partition)
		partition := collector.summary.TopicPartitions[partitionKey]
		partition.Acknowledged++
		partition.LastOffset = result.offset
		if partition.FirstAcknowledgedEpochMicros == 0 {
			partition.FirstAcknowledgedEpochMicros = acknowledgedAt
		}
		partition.LastAcknowledgedEpochMicros = acknowledgedAt
		collector.summary.TopicPartitions[partitionKey] = partition
		if result.sourceLine > 0 {
			last := collector.tracker.acknowledge(result.sourceLine)
			collector.summary.LastContiguousAcknowledgedLine = last
			collector.checkpoint.LastContiguousAcknowledged = last
			collector.checkpoint.LastAcknowledgedSourceLine = last
			if last >= collector.lastSaved+collector.checkpointEvery {
				if err := saveJSONAtomic(collector.checkpointPath, collector.checkpoint); err != nil && collector.err == nil {
					collector.err = fmt.Errorf("persist checkpoint: %w", err)
				}
				collector.lastSaved = last
			}
		}
		collector.mu.Unlock()
	}
}

func (collector *deliveryCollector) progressSnapshot() (uint64, int64, int64, map[string]PartitionSummary) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	partitions := make(map[string]PartitionSummary, len(collector.summary.TopicPartitions))
	for key, value := range collector.summary.TopicPartitions {
		partitions[key] = value
	}
	return collector.summary.LastContiguousAcknowledgedLine,
		collector.summary.FirstAcknowledgedEpochMicros,
		collector.summary.LastAcknowledgedEpochMicros,
		partitions
}

func (collector *deliveryCollector) result() error {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.err != nil {
		return collector.err
	}
	if collector.summary.Failed > 0 {
		sort.Strings(collector.summary.Errors)
		return fmt.Errorf("%d Kafka records failed acknowledgement", collector.summary.Failed)
	}
	return nil
}

func (collector *deliveryCollector) acknowledgedSourceRecords() uint64 {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	return collector.summary.LastContiguousAcknowledgedLine
}
