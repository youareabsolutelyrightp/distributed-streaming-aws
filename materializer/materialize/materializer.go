package materialize

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Result struct {
	RunID                       string          `json:"run_id"`
	OutputID                    string          `json:"output_id"`
	CanonicalDigest             string          `json:"-"`
	QueryID                     string          `json:"query_id"`
	GradingMode                 string          `json:"grading_mode"`
	EffectiveTime               time.Time       `json:"effective_time"`
	Terminal                    bool            `json:"terminal"`
	Raw                         json.RawMessage `json:"-"`
	TransportMeasurementVersion int             `json:"transport_measurement_version"`
}

type serializationMeasurement struct {
	StartedEpochMicros   *int64
	CompletedEpochMicros *int64
	Trace                json.RawMessage
}

type Summary struct {
	Read                         uint64            `json:"read_records"`
	Inserted                     uint64            `json:"inserted_records"`
	Duplicates                   uint64            `json:"duplicate_records"`
	IdenticalDuplicateAttempts   uint64            `json:"identical_duplicate_attempts"`
	ConflictingDuplicateAttempts uint64            `json:"conflicting_duplicate_attempts"`
	Batches                      uint64            `json:"committed_batches"`
	DatabaseRetries              uint64            `json:"database_retries"`
	BatchSizes                   batchSizeSummary  `json:"batch_size"`
	DatabaseExecution            durationHistogram `json:"database_execution_latency"`
	DatabaseTransactionCommit    durationHistogram `json:"database_transaction_commit_latency"`
	KafkaOffsetCommit            durationHistogram `json:"kafka_offset_commit_latency"`
}

type batchSizeSummary struct {
	Count   uint64  `json:"count"`
	Sum     uint64  `json:"sum"`
	Average float64 `json:"average"`
	Minimum int     `json:"minimum"`
	Maximum int     `json:"maximum"`
	P50     int     `json:"p50"`
	P95     int     `json:"p95"`
	P99     int     `json:"p99"`
	counts  []uint64
}

func (summary *batchSizeSummary) observe(size int) {
	if size <= 0 {
		return
	}
	if len(summary.counts) <= size {
		next := make([]uint64, size+1)
		copy(next, summary.counts)
		summary.counts = next
	}
	summary.counts[size]++
	summary.Count++
	summary.Sum += uint64(size)
	if summary.Minimum == 0 || size < summary.Minimum {
		summary.Minimum = size
	}
	if size > summary.Maximum {
		summary.Maximum = size
	}
	summary.refresh()
}

func (summary *batchSizeSummary) refresh() {
	if summary.Count == 0 {
		return
	}
	summary.Average = float64(summary.Sum) / float64(summary.Count)
	summary.P50 = summary.percentile(0.50)
	summary.P95 = summary.percentile(0.95)
	summary.P99 = summary.percentile(0.99)
}

func (summary *batchSizeSummary) percentile(fraction float64) int {
	target := uint64(float64(summary.Count)*fraction + 0.999999999)
	if target < 1 {
		target = 1
	}
	var observed uint64
	for size, count := range summary.counts {
		observed += count
		if observed >= target {
			return size
		}
	}
	return summary.Maximum
}

type durationHistogram struct {
	Count   uint64            `json:"count"`
	SumMS   float64           `json:"sum_ms"`
	Maximum float64           `json:"maximum_ms"`
	Buckets map[string]uint64 `json:"cumulative_buckets"`
}

func newDurationHistogram() durationHistogram {
	buckets := make(map[string]uint64)
	for _, bound := range []int{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000} {
		buckets[fmt.Sprintf("le_%d_ms", bound)] = 0
	}
	buckets["gt_2000_ms"] = 0
	return durationHistogram{Buckets: buckets}
}

func (histogram *durationHistogram) observe(duration time.Duration) {
	milliseconds := float64(duration) / float64(time.Millisecond)
	histogram.Count++
	histogram.SumMS += milliseconds
	if milliseconds > histogram.Maximum {
		histogram.Maximum = milliseconds
	}
	matched := false
	for _, bound := range []int{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000} {
		if milliseconds <= float64(bound) {
			histogram.Buckets[fmt.Sprintf("le_%d_ms", bound)]++
			matched = true
		}
	}
	if !matched {
		histogram.Buckets["gt_2000_ms"]++
	}
}

func Run(ctx context.Context, config Config) (Summary, error) {
	summary := Summary{
		DatabaseExecution:         newDurationHistogram(),
		DatabaseTransactionCommit: newDurationHistogram(),
		KafkaOffsetCommit:         newDurationHistogram(),
	}
	pool, err := pgxpool.New(ctx, config.DatabaseURL)
	if err != nil {
		return summary, fmt.Errorf("open TimescaleDB: %w", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "SELECT 1 FROM canonical_results LIMIT 0"); err != nil {
		return summary, fmt.Errorf("verify materializer schema migration: %w", err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(config.Brokers...),
		kgo.ConsumerGroup(config.GroupID),
		kgo.ConsumeTopics(config.Query1Topic, config.Query2Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return summary, fmt.Errorf("open Kafka consumer: %w", err)
	}
	defer client.Close()

	lastRecord := time.Now()
	batch := newRecordBatch(config.BatchSize, config.FlushInterval)
	for {
		pollTimeout := config.FlushInterval
		if !batch.empty() {
			pollTimeout = batch.remaining(time.Now())
			if pollTimeout <= 0 {
				pollTimeout = time.Nanosecond
			}
		}
		pollContext, cancel := context.WithTimeout(ctx, pollTimeout)
		fetches := client.PollRecords(pollContext, batch.capacity())
		cancel()
		if errors.Is(fetches.Err(), context.Canceled) && ctx.Err() != nil {
			return summary, ctx.Err()
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return summary, fmt.Errorf("poll Kafka: %w", err)
		}
		records := fetches.Records()
		if len(records) > 0 {
			batch.add(records, time.Now())
			lastRecord = time.Now()
		}
		if !batch.ready(time.Now()) {
			if config.ExitAfterIdle > 0 && time.Since(lastRecord) >= config.ExitAfterIdle {
				return summary, nil
			}
			continue
		}
		records = batch.take()
		write, retries, err := retryCommitBatch(
			ctx, config.RetryAttempts, config.RetryBackoff,
			func() (batchWriteResult, error) { return commitBatch(ctx, pool, records) })
		if err != nil {
			var conflict canonicalConflictError
			if errors.As(err, &conflict) {
				summary.ConflictingDuplicateAttempts++
			}
			return summary, err
		}
		summary.DatabaseRetries += uint64(retries)
		summary.DatabaseExecution.observe(write.Execution)
		summary.DatabaseTransactionCommit.observe(write.Commit)
		offsetCommitStarted := time.Now()
		if err := client.CommitRecords(ctx, records...); err != nil {
			return summary, fmt.Errorf("commit Kafka offsets after database commit: %w", err)
		}
		summary.KafkaOffsetCommit.observe(time.Since(offsetCommitStarted))
		summary.Read += uint64(len(records))
		summary.Inserted += uint64(write.Inserted)
		summary.Duplicates += uint64(write.IdenticalDuplicates)
		summary.IdenticalDuplicateAttempts += uint64(write.IdenticalDuplicates)
		summary.Batches++
		summary.BatchSizes.observe(len(records))
		if config.ProgressPath != "" {
			if err := saveSummary(config.ProgressPath, summary); err != nil {
				return summary, fmt.Errorf("write materializer progress: %w", err)
			}
		}
	}
}

type recordBatch struct {
	records       []*kgo.Record
	started       time.Time
	limit         int
	flushInterval time.Duration
}

func newRecordBatch(limit int, flushInterval time.Duration) *recordBatch {
	return &recordBatch{limit: limit, flushInterval: flushInterval}
}

func (batch *recordBatch) empty() bool { return len(batch.records) == 0 }

func (batch *recordBatch) capacity() int {
	remaining := batch.limit - len(batch.records)
	if remaining < 1 {
		return 1
	}
	return remaining
}

func (batch *recordBatch) add(records []*kgo.Record, now time.Time) {
	if len(records) == 0 {
		return
	}
	if batch.empty() {
		batch.started = now
	}
	batch.records = append(batch.records, records...)
}

func (batch *recordBatch) remaining(now time.Time) time.Duration {
	if batch.empty() {
		return batch.flushInterval
	}
	return batch.flushInterval - now.Sub(batch.started)
}

func (batch *recordBatch) ready(now time.Time) bool {
	return len(batch.records) >= batch.limit ||
		(!batch.empty() && batch.remaining(now) <= 0)
}

func (batch *recordBatch) take() []*kgo.Record {
	records := batch.records
	batch.records = nil
	batch.started = time.Time{}
	return records
}

func saveSummary(path string, summary Summary) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".materializer-*.json")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err := temporary.Write(append(data, '\n')); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

type batchWriteResult struct {
	Inserted            int64
	IdenticalDuplicates int64
	Execution           time.Duration
	Commit              time.Duration
}

func retryCommitBatch(
	ctx context.Context,
	attempts int,
	backoff time.Duration,
	commit func() (batchWriteResult, error),
) (batchWriteResult, int, error) {
	var result batchWriteResult
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		result, err = commit()
		if err == nil {
			return result, attempt - 1, nil
		}
		if attempt == attempts {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return batchWriteResult{}, attempt - 1, ctx.Err()
		case <-timer.C:
		}
	}
	return batchWriteResult{}, attempts - 1, err
}

func commitBatch(ctx context.Context, pool *pgxpool.Pool, records []*kgo.Record) (batchWriteResult, error) {
	decoded := make([]Result, 0, len(records))
	measurements := make([]serializationMeasurement, 0, len(records))
	for _, record := range records {
		result, err := decodeResult(record.Value)
		if err != nil {
			return batchWriteResult{}, fmt.Errorf(
				"decode %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
		}
		measurement, err := decodeSerializationMeasurement(record, result.TransportMeasurementVersion)
		if err != nil {
			return batchWriteResult{}, fmt.Errorf(
				"decode transport headers %s[%d]@%d: %w",
				record.Topic, record.Partition, record.Offset, err)
		}
		decoded = append(decoded, result)
		measurements = append(measurements, measurement)
	}
	executionStarted := time.Now()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return batchWriteResult{}, fmt.Errorf("begin materializer batch: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	batch := &pgx.Batch{}
	for index, result := range decoded {
		record := records[index]
		batch.Queue(`
INSERT INTO canonical_results (
    run_id, output_id, canonical_digest, query_id, grading_mode, effective_time, terminal, result,
    kafka_topic, kafka_partition, kafka_offset, kafka_append_time,
    serialization_started_epoch_us, serialization_completed_epoch_us,
    kafka_key_bytes, kafka_value_bytes, kafka_header_bytes, transport_trace, kafka_value_raw
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
ON CONFLICT (run_id, output_id) DO NOTHING`,
			result.RunID, result.OutputID, result.CanonicalDigest,
			result.QueryID, result.GradingMode,
			result.EffectiveTime, result.Terminal, result.Raw,
			record.Topic, record.Partition, record.Offset, record.Timestamp,
			measurements[index].StartedEpochMicros,
			measurements[index].CompletedEpochMicros,
			len(record.Key), len(record.Value), kafkaHeaderBytes(record.Headers),
			measurements[index].Trace, record.Value)
	}
	results := tx.SendBatch(ctx, batch)
	var inserted int64
	duplicateIndexes := make([]int, 0)
	for index := range records {
		tag, err := results.Exec()
		if err != nil {
			_ = results.Close()
			return batchWriteResult{}, fmt.Errorf("upsert canonical result batch: %w", err)
		}
		inserted += tag.RowsAffected()
		if tag.RowsAffected() == 0 {
			duplicateIndexes = append(duplicateIndexes, index)
		}
	}
	if err := results.Close(); err != nil {
		return batchWriteResult{}, fmt.Errorf("close canonical result batch: %w", err)
	}
	if len(duplicateIndexes) > 0 {
		verification := &pgx.Batch{}
		for _, index := range duplicateIndexes {
			result := decoded[index]
			verification.Queue(
				"SELECT canonical_digest FROM canonical_results WHERE run_id=$1 AND output_id=$2",
				result.RunID, result.OutputID)
		}
		verificationResults := tx.SendBatch(ctx, verification)
		for _, index := range duplicateIndexes {
			var existingDigest string
			if err := verificationResults.QueryRow().Scan(&existingDigest); err != nil {
				_ = verificationResults.Close()
				return batchWriteResult{}, fmt.Errorf("read existing canonical digest: %w", err)
			}
			if existingDigest != decoded[index].CanonicalDigest {
				_ = verificationResults.Close()
				return batchWriteResult{}, canonicalConflictError{
					runID: decoded[index].RunID, outputID: decoded[index].OutputID}
			}
		}
		if err := verificationResults.Close(); err != nil {
			return batchWriteResult{}, fmt.Errorf("close canonical conflict verification: %w", err)
		}
	}
	execution := time.Since(executionStarted)
	commitStarted := time.Now()
	if err := tx.Commit(ctx); err != nil {
		return batchWriteResult{}, fmt.Errorf("commit materializer batch: %w", err)
	}
	return batchWriteResult{
		Inserted: inserted, IdenticalDuplicates: int64(len(duplicateIndexes)),
		Execution: execution, Commit: time.Since(commitStarted),
	}, nil
}

func kafkaHeaderBytes(headers []kgo.RecordHeader) int {
	total := 0
	for _, header := range headers {
		total += len(header.Key) + len(header.Value)
	}
	return total
}

func decodeSerializationMeasurement(
	record *kgo.Record, transportVersion int,
) (serializationMeasurement, error) {
	traceV3 := lastHeader(record, "transport_trace_v3")
	if traceV3 != nil {
		if transportVersion != 3 {
			return serializationMeasurement{}, errors.New("full transport header requires version 3 marker")
		}
		return decodeFullTransport(traceV3)
	}
	if transportVersion > 0 {
		return serializationMeasurement{}, errors.New("Transport v3 marker requires transport_trace_v3")
	}
	return serializationMeasurement{}, nil
}

func lastHeader(record *kgo.Record, key string) []byte {
	var value []byte
	for _, header := range record.Headers {
		if header.Key == key {
			value = header.Value
		}
	}
	return value
}

func decodeFullTransport(value []byte) (serializationMeasurement, error) {
	if len(value) != 112 || value[0] != 3 || value[3] != 0 ||
		binary.BigEndian.Uint32(value[4:8]) != 0 || binary.BigEndian.Uint32(value[108:112]) != 0 {
		return serializationMeasurement{}, errors.New("invalid transport_trace_v3 header")
	}
	version := 3
	flags := value[1]
	if flags&^byte(7) != 0 {
		return serializationMeasurement{}, errors.New("full transport header has invalid flags")
	}
	read64 := func(offset int) int64 { return int64(binary.BigEndian.Uint64(value[offset : offset+8])) }
	partition := int8(value[2])
	sourceLine := binary.BigEndian.Uint64(value[8:16])
	firstAppend := read64(16)
	triggerAppend := read64(24)
	triggerRead := read64(32)
	maximumRead := read64(40)
	partitionBoundaryEmitted := int64(0)
	sharedBoundaryEmitted := int64(0)
	partitionBoundaryEmitted = read64(48)
	sharedBoundaryEmitted = read64(56)
	boundaryOffset := 64
	boundaryArrival := read64(boundaryOffset)
	watermarkClosed := read64(boundaryOffset + 8)
	globalStart := read64(boundaryOffset + 16)
	emitted := read64(boundaryOffset + 24)
	globalDuration := int64(binary.BigEndian.Uint32(value[boundaryOffset+32 : boundaryOffset+36]))
	handoffDuration := int64(binary.BigEndian.Uint32(value[boundaryOffset+36 : boundaryOffset+40]))
	serializationDuration := int64(binary.BigEndian.Uint32(value[boundaryOffset+40 : boundaryOffset+44]))
	if err := validateTriggerIdentity(flags, partition, sourceLine, uint64(triggerAppend)); err != nil {
		return serializationMeasurement{}, err
	}
	clockResolutionCorrections := 0
	if flags&1 != 0 {
		ordered := []int64{firstAppend, triggerAppend, triggerRead, maximumRead}
		names := []string{"first_append", "trigger_append", "trigger_read", "maximum_read"}
		ordered = append(ordered, partitionBoundaryEmitted, sharedBoundaryEmitted)
		names = append(names, "partition_boundary_emitted", "shared_boundary_emitted")
		ordered = append(ordered, boundaryArrival, watermarkClosed, globalStart, emitted)
		names = append(names, "boundary_arrival", "watermark_closed", "global_start", "emitted")
		for index, timestamp := range ordered {
			if timestamp <= 0 {
				return serializationMeasurement{}, fmt.Errorf(
					"full transport header has impossible input time ordering at %s: %v",
					names[index], ordered,
				)
			}
			if index > 0 && timestamp < ordered[index-1] {
				if ordered[index-1]-timestamp > 1_000 {
					return serializationMeasurement{}, fmt.Errorf(
						"full transport header has impossible input time ordering at %s: %v",
						names[index], ordered,
					)
				}
				ordered[index] = ordered[index-1]
				clockResolutionCorrections++
			}
		}
		firstAppend, triggerAppend = ordered[0], ordered[1]
		triggerRead, maximumRead = ordered[2], ordered[3]
		offset := 4
		partitionBoundaryEmitted, sharedBoundaryEmitted = ordered[4], ordered[5]
		offset = 6
		boundaryArrival, watermarkClosed = ordered[offset], ordered[offset+1]
		globalStart, emitted = ordered[offset+2], ordered[offset+3]
	} else if watermarkClosed > 0 {
		if globalStart < watermarkClosed || emitted < globalStart {
			return serializationMeasurement{}, errors.New("full transport header has impossible timer time ordering")
		}
	}
	if globalStart > 0 && emitted-globalStart != globalDuration {
		return serializationMeasurement{}, errors.New("global duration does not match transport epochs")
	}
	started := emitted + handoffDuration
	completed := started + serializationDuration
	trace, err := json.Marshal(map[string]any{
		"transport_measurement_version":         version,
		"trigger_flags":                         flags,
		"trigger_input_partition":               partition,
		"trigger_input_source_line":             sourceLine,
		"boundary_first_input_append_epoch_us":  firstAppend,
		"trigger_input_append_epoch_us":         triggerAppend,
		"trigger_input_source_read_epoch_us":    triggerRead,
		"maximum_required_source_read_epoch_us": maximumRead,
		"partition_boundary_emitted_epoch_us":   partitionBoundaryEmitted,
		"shared_boundary_emitted_epoch_us":      sharedBoundaryEmitted,
		"boundary_signal_arrival_epoch_us":      boundaryArrival,
		"watermark_closed_epoch_us":             watermarkClosed,
		"global_boundary_started_epoch_us":      globalStart,
		"result_emitted_epoch_us":               emitted,
		"global_computation_duration_us":        globalDuration,
		"result_handoff_duration_us":            handoffDuration,
		"serialization_duration_us":             serializationDuration,
		"clock_resolution_corrections":          clockResolutionCorrections,
	})
	if err != nil {
		return serializationMeasurement{}, err
	}
	return serializationMeasurement{StartedEpochMicros: &started, CompletedEpochMicros: &completed, Trace: trace}, nil
}

func validateTriggerIdentity(flags byte, partition int8, sourceLine, appendTime uint64) error {
	hasInput := flags&1 != 0
	if hasInput && (partition < 0 || sourceLine == 0 || appendTime == 0) {
		return errors.New("transport header has incomplete input trigger identity")
	}
	if !hasInput && (partition != -1 || sourceLine != 0 || appendTime != 0) {
		return errors.New("transport header has input identity without input flag")
	}
	if flags&7 == 0 {
		return errors.New("transport header has no trigger flag")
	}
	return nil
}

type canonicalConflictError struct {
	runID    string
	outputID string
}

func (errorValue canonicalConflictError) Error() string {
	return fmt.Sprintf(
		"output_id %s for run %s has conflicting canonical content",
		errorValue.outputID, errorValue.runID)
}

func decodeResult(value []byte) (Result, error) {
	var result Result
	if err := json.Unmarshal(value, &result); err != nil {
		return result, err
	}
	if result.RunID == "" || len(result.OutputID) != 64 ||
		(result.QueryID != "query1" && result.QueryID != "query2") ||
		result.GradingMode != "event-time-state" ||
		result.EffectiveTime.IsZero() {
		return result, errors.New("result is missing its canonical identity")
	}
	digest, err := canonicalDigest(value)
	if err != nil {
		return result, fmt.Errorf("calculate canonical digest: %w", err)
	}
	if digest != result.OutputID {
		return result, errors.New("output_id does not match canonical format version 1 bytes")
	}
	result.CanonicalDigest = digest
	result.Raw = append(result.Raw[:0], value...)
	return result, nil
}
