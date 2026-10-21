package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrPaused = errors.New("replay paused after a requested source line")

type controlRecord struct {
	RecordType           string  `json:"record_type"`
	SchemaVersion        int     `json:"schema_version"`
	ControlID            string  `json:"control_id"`
	RunID                string  `json:"run_id"`
	DatasetID            string  `json:"dataset_id"`
	GradingMode          string  `json:"grading_mode"`
	InputTopic           string  `json:"input_topic"`
	ExpectedPartitions   int     `json:"expected_partition_count"`
	PartitioningVersion  string  `json:"partitioning_version"`
	PartitionHashVersion string  `json:"partition_hash_test_vector_version"`
	ExpectedSourceCount  uint64  `json:"expected_source_count"`
	MaximumEventTime     string  `json:"maximum_event_time"`
	PreparedChecksum     string  `json:"prepared_checksum"`
	RequestedRatePerSec  float64 `json:"requested_rate_per_second"`
	CreatedAtEpochMicros int64   `json:"created_at_epoch_us"`
	EOSAcknowledged      int     `json:"eos_acknowledged,omitempty"`
	LastAcknowledgedLine uint64  `json:"last_acknowledged_source_line,omitempty"`
}

type eosRecord struct {
	RecordType             string `json:"record_type"`
	SchemaVersion          int    `json:"schema_version"`
	RunID                  string `json:"run_id"`
	DatasetID              string `json:"dataset_id"`
	Partition              int    `json:"partition"`
	ExpectedPartitions     int    `json:"expected_partition_count"`
	ExpectedSourceCount    uint64 `json:"expected_source_count"`
	LastSourceLine         uint64 `json:"last_source_line"`
	MaximumEventTime       string `json:"maximum_event_time"`
	ProducerSendEpochMicro int64  `json:"producer_send_epoch_us"`
}

func newKafkaClient(config Config) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(config.Brokers...),
		kgo.ClientID("reconstructed-foil-replay-v1"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.MaxBufferedRecords(100000),
		kgo.RecordDeliveryTimeout(2*time.Minute),
	)
}

func stableControlID(runID, recordType string) string {
	digest := sha256.Sum256([]byte(runID + "|" + recordType))
	return hex.EncodeToString(digest[:])
}

func appendRunError(summary *Summary, runErr error) {
	if runErr == nil || errors.Is(runErr, ErrPaused) {
		return
	}
	message := runErr.Error()
	for _, existing := range summary.Errors {
		if existing == message {
			return
		}
	}
	summary.Errors = append(summary.Errors, message)
}

func Run(ctx context.Context, config Config) (*Summary, error) {
	startedAt := time.Now()
	manifest, err := LoadManifest(config.ManifestPath)
	if err != nil {
		return nil, err
	}
	if err := config.normalize(manifest); err != nil {
		return nil, err
	}
	inputTopic, partitionCount := config.inputTopicAndPartitions()
	maximumEventTime := manifest.MaximumEventTime
	if config.MaximumEventTime != "" {
		maximumEventTime = config.MaximumEventTime
	}
	summary := &Summary{
		Version: 4, RunID: config.RunID, DatasetID: manifest.DatasetID,
		Profile: BenchmarkProfile, GradingMode: BenchmarkGradingMode, InputTopic: inputTopic,
		ExpectedPartitionCount: partitionCount, ExpectedSourceCount: config.Limit,
		PartitioningVersion:        PartitioningVersion,
		PartitionHashVectorVersion: PartitionHashVectorVersion,
		RequestedRatePerSecond:     config.requestedRate(),
		ProgressPhase:              config.ProgressPhase,
		AcknowledgementLatency:     newLatencyHistogram(),
		TopicPartitions:            make(map[string]PartitionSummary),
	}
	var progress *progressRecorder
	finish := func(runErr error) (*Summary, error) {
		if progressErr := progress.close(); progressErr != nil {
			if runErr == nil {
				runErr = progressErr
			} else {
				summary.Errors = append(summary.Errors, progressErr.Error())
			}
		}
		summary.ElapsedSeconds = time.Since(startedAt).Seconds()
		processed := summary.PrimaryRecordsQueued
		acknowledgementSeconds := float64(
			summary.LastAcknowledgedEpochMicros-summary.FirstAcknowledgedEpochMicros,
		) / float64(time.Second/time.Microsecond)
		if processed > 1 && acknowledgementSeconds > 0 {
			summary.SourceRecordsPerSecond = float64(processed) / acknowledgementSeconds
		} else if summary.ElapsedSeconds > 0 {
			summary.SourceRecordsPerSecond = float64(processed) / summary.ElapsedSeconds
		}
		appendRunError(summary, runErr)
		if saveErr := saveJSONAtomic(config.SummaryPath, summary); saveErr != nil {
			if runErr == nil {
				runErr = fmt.Errorf("write replay summary: %w", saveErr)
			} else {
				summary.Errors = append(summary.Errors, saveErr.Error())
			}
		}
		return summary, runErr
	}

	checkpoint := Checkpoint{
		Version: 4, RunID: config.RunID, DatasetID: manifest.DatasetID,
		ReplayConfigSHA256:         replayConfigDigest(config, manifest),
		PreparedChecksum:           manifest.PreparedChecksum,
		PartitioningVersion:        PartitioningVersion,
		PartitionHashVectorVersion: PartitionHashVectorVersion,
		InputPartitionCount:        partitionCount,
	}
	if config.Resume {
		checkpoint, err = loadCheckpoint(config.CheckpointPath)
		if err != nil {
			return finish(fmt.Errorf("load resume checkpoint: %w", err))
		}
		if err := validateCheckpoint(checkpoint, config, manifest); err != nil {
			return finish(err)
		}
		summary.ResumedFromSourceLine = checkpoint.LastAcknowledgedSourceLine
	} else {
		if _, statErr := os.Stat(config.CheckpointPath); statErr == nil {
			return finish(errors.New("checkpoint already exists; choose a new run-id or use --resume"))
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return finish(fmt.Errorf("inspect checkpoint: %w", statErr))
		}
		if err := saveJSONAtomic(config.CheckpointPath, checkpoint); err != nil {
			return finish(fmt.Errorf("create checkpoint: %w", err))
		}
	}

	client, err := newKafkaClient(config)
	if err != nil {
		return finish(fmt.Errorf("create Kafka producer: %w", err))
	}
	defer client.Close()

	if !checkpoint.Started {
		start := controlRecord{
			RecordType: "run_start", SchemaVersion: 1,
			ControlID: stableControlID(config.RunID, "run_start"), RunID: config.RunID,
			DatasetID: manifest.DatasetID, GradingMode: BenchmarkGradingMode, InputTopic: inputTopic,
			ExpectedPartitions: partitionCount, ExpectedSourceCount: config.Limit,
			PartitioningVersion:  PartitioningVersion,
			PartitionHashVersion: PartitionHashVectorVersion,
			MaximumEventTime:     maximumEventTime, PreparedChecksum: manifest.PreparedChecksum,
			RequestedRatePerSec:  config.requestedRate(),
			CreatedAtEpochMicros: time.Now().UnixMicro(),
		}
		if err := produceSyncJSON(ctx, client, config.ControlTopic, []byte(config.RunID), 0, start); err != nil {
			return finish(fmt.Errorf("acknowledge run-start control record: %w", err))
		}
		summary.ControlAcknowledged++
		checkpoint.Started = true
		if err := saveJSONAtomic(config.CheckpointPath, checkpoint); err != nil {
			return finish(fmt.Errorf("persist started checkpoint: %w", err))
		}
	}

	source, err := OpenPrepared(config.InputPath)
	if err != nil {
		return finish(err)
	}
	defer source.Close()

	resumeFrom := checkpoint.LastContiguousAcknowledged
	tracker := newAckTracker(resumeFrom)
	deliveries := make(chan delivery, 131072)
	collectorDone := make(chan struct{})
	collector := &deliveryCollector{
		summary: summary, tracker: tracker, checkpoint: &checkpoint,
		checkpointPath: config.CheckpointPath, checkpointEvery: config.CheckpointEvery,
		lastSaved: resumeFrom,
	}
	go collector.collect(deliveries, collectorDone)

	progress, err = startProgressRecorder(config.ProgressPath, func() progressRecord {
		acknowledged, firstAcknowledged, lastAcknowledged, partitions := collector.progressSnapshot()
		phaseAcknowledged := uint64(0)
		if acknowledged > resumeFrom {
			phaseAcknowledged = acknowledged - resumeFrom
		}
		return progressRecord{
			Phase:                        config.ProgressPhase,
			AcknowledgedSourceRecords:    acknowledged,
			PhaseAcknowledgedRecords:     phaseAcknowledged,
			FirstAcknowledgedEpochMicros: firstAcknowledged,
			LastAcknowledgedEpochMicros:  lastAcknowledged,
			CurrentRequestedRate:         config.Rate,
			TopicPartitions:              partitions,
		}
	})
	if err != nil {
		client.Flush(context.Background())
		close(deliveries)
		<-collectorDone
		return finish(fmt.Errorf("create producer progress: %w", err))
	}

	queuedThisInvocation := uint64(0)
	partitionsByMedallion := make(map[string]int32)
	paused := false
	var limiterStart time.Time
	for {
		ingestMicros := time.Now().UnixMicro()
		event, readErr := source.Read(config.RunID, ingestMicros)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			client.Flush(ctx)
			close(deliveries)
			<-collectorDone
			return finish(fmt.Errorf("read prepared event: %w", readErr))
		}
		if event.DatasetID != manifest.DatasetID {
			client.Flush(ctx)
			close(deliveries)
			<-collectorDone
			return finish(fmt.Errorf("source line %d has the wrong dataset_id", event.SourceLineNumber))
		}
		if event.SourceLineNumber <= resumeFrom {
			continue
		}
		if event.SourceLineNumber > config.Limit {
			break
		}
		if summary.FirstSourceLine == 0 {
			summary.FirstSourceLine = event.SourceLineNumber
			summary.FirstEventTime = event.DropoffDatetime
		}
		summary.LastSourceLine = event.SourceLineNumber
		summary.LastEventTime = event.DropoffDatetime
		if config.Rate > 0 {
			if limiterStart.IsZero() {
				limiterStart = time.Now()
			}
			target := limiterStart.Add(time.Duration(float64(queuedThisInvocation) / config.Rate * float64(time.Second)))
			if wait := time.Until(target); wait > 0 {
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					client.Flush(context.Background())
					close(deliveries)
					<-collectorDone
					return finish(ctx.Err())
				}
			}
		}
		partition, exists := partitionsByMedallion[event.Medallion]
		if !exists {
			partition, err = PartitionForMedallion(event.Medallion, partitionCount)
			if err != nil {
				return finish(fmt.Errorf("partition source line %d: %w", event.SourceLineNumber, err))
			}
			partitionsByMedallion[event.Medallion] = partition
		}
		event.ProducerSendEpochMicros = time.Now().UnixMicro()
		payload, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			return finish(fmt.Errorf("encode source line %d: %w", event.SourceLineNumber, marshalErr))
		}
		record := &kgo.Record{Topic: inputTopic, Key: []byte(event.Medallion), Value: payload, Partition: partition}
		produceAsync(ctx, client, record, event.SourceLineNumber, deliveries)
		summary.PrimaryRecordsQueued++
		queuedThisInvocation++
		if config.PauseAfterRecord > 0 && event.SourceLineNumber >= config.PauseAfterRecord {
			paused = true
			break
		}
	}

	if err := client.Flush(ctx); err != nil {
		close(deliveries)
		<-collectorDone
		return finish(fmt.Errorf("flush Kafka data records: %w", err))
	}
	close(deliveries)
	<-collectorDone
	if err := collector.result(); err != nil {
		return finish(err)
	}
	if err := saveJSONAtomic(config.CheckpointPath, checkpoint); err != nil {
		return finish(fmt.Errorf("persist final data checkpoint: %w", err))
	}
	if paused {
		summary.Paused = true
		return finish(ErrPaused)
	}
	if checkpoint.LastContiguousAcknowledged != config.Limit {
		return finish(fmt.Errorf("acknowledged through source line %d, expected %d", checkpoint.LastContiguousAcknowledged, config.Limit))
	}
	manualClient, err := newKafkaClient(config)
	if err != nil {
		return finish(fmt.Errorf("create EOS producer: %w", err))
	}
	sendEOS := func(partition int) error {
		eos := eosRecord{
			RecordType: "eos", SchemaVersion: 1, RunID: config.RunID,
			DatasetID: manifest.DatasetID, Partition: partition,
			ExpectedPartitions: partitionCount, ExpectedSourceCount: config.Limit,
			LastSourceLine:         summary.LastSourceLine,
			MaximumEventTime:       maximumEventTime,
			ProducerSendEpochMicro: time.Now().UnixMicro(),
		}
		key := []byte(fmt.Sprintf("__eos__:%s:%d", config.RunID, partition))
		if err := produceSyncJSON(ctx, manualClient, inputTopic, key, int32(partition), eos); err != nil {
			return fmt.Errorf("acknowledge EOS for partition %d: %w", partition, err)
		}
		summary.EOSAcknowledged++
		return nil
	}
	for partition := 0; partition < partitionCount; partition++ {
		if err := sendEOS(partition); err != nil {
			manualClient.Close()
			return finish(err)
		}
	}
	manualClient.Close()

	complete := controlRecord{
		RecordType: "producer_complete", SchemaVersion: 1,
		ControlID: stableControlID(config.RunID, "producer_complete"), RunID: config.RunID,
		DatasetID: manifest.DatasetID, GradingMode: BenchmarkGradingMode, InputTopic: inputTopic,
		ExpectedPartitions: partitionCount, ExpectedSourceCount: config.Limit,
		PartitioningVersion:  PartitioningVersion,
		PartitionHashVersion: PartitionHashVectorVersion,
		MaximumEventTime:     maximumEventTime, PreparedChecksum: manifest.PreparedChecksum,
		RequestedRatePerSec:  config.requestedRate(),
		CreatedAtEpochMicros: time.Now().UnixMicro(),
		EOSAcknowledged:      int(summary.EOSAcknowledged), LastAcknowledgedLine: checkpoint.LastAcknowledgedSourceLine,
	}
	if err := produceSyncJSON(ctx, client, config.ControlTopic, []byte(config.RunID), 0, complete); err != nil {
		return finish(fmt.Errorf("acknowledge producer-complete record: %w", err))
	}
	summary.ControlAcknowledged++
	checkpoint.Completed = true
	if err := saveJSONAtomic(config.CheckpointPath, checkpoint); err != nil {
		return finish(fmt.Errorf("persist completed checkpoint: %w", err))
	}
	summary.Completed = true
	return finish(nil)
}

func produceAsync(ctx context.Context, client *kgo.Client, record *kgo.Record, sourceLine uint64, results chan<- delivery) {
	sentAt := time.Now()
	client.Produce(ctx, record, func(delivered *kgo.Record, err error) {
		results <- delivery{
			sourceLine: sourceLine, sentAt: sentAt,
			topic: delivered.Topic, partition: delivered.Partition, offset: delivered.Offset,
			acknowledgedAt: time.Now(), err: err,
		}
	})
}

func produceSyncJSON(ctx context.Context, client *kgo.Client, topic string, key []byte, partition int32, value any) error {
	if partition < 0 {
		return fmt.Errorf("partition must be non-negative, got %d", partition)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	record := &kgo.Record{Topic: topic, Key: key, Value: payload, Partition: partition, Timestamp: time.Now()}
	return client.ProduceSync(ctx, record).FirstErr()
}
