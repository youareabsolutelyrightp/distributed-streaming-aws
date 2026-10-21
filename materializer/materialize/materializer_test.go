package materialize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDecodeResultRequiresCanonicalIdentity(t *testing.T) {
	value := validResultPayload(t, "query2", false)
	result, err := decodeResult(value)
	if err != nil {
		t.Fatal(err)
	}
	if result.RunID != "run" || result.CanonicalDigest != result.OutputID ||
		string(result.Raw) != string(value) {
		t.Fatalf("unexpected result: %#v", result)
	}
	if _, err := decodeResult([]byte(`{"run_id":"run"}`)); err == nil {
		t.Fatal("invalid result was accepted")
	}
}

func TestCanonicalDigestMatchesVersionOneOutputContract(t *testing.T) {
	value := validResultPayload(t, "query1", false)
	result, err := decodeResult(value)
	if err != nil {
		t.Fatal(err)
	}
	var changed map[string]any
	if err := json.Unmarshal(value, &changed); err != nil {
		t.Fatal(err)
	}
	changed["terminal"] = true
	conflict, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeResult(conflict); err == nil {
		t.Fatal("changed canonical content kept the old output ID")
	}
	line, err := canonicalBaseLine(value)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(line, []byte(`"route_id":"1.1>2.2"`)) {
		t.Fatalf("canonical line escaped or removed the route ID: %s", line)
	}
	if result.OutputID == "" {
		t.Fatal("canonical output ID is empty")
	}
}

func TestCanonicalGoldenVectors(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate materializer contract tests")
	}
	data, err := os.ReadFile(filepath.Join(
		filepath.Dir(source), "..", "..", "schemas", "canonical-test-vectors.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		var vector struct {
			Name                  string          `json:"name"`
			Record                json.RawMessage `json:"record"`
			ExpectedCanonicalLine string          `json:"expected_canonical_line"`
			ExpectedOutputID      string          `json:"expected_output_id"`
		}
		if err := json.Unmarshal(line, &vector); err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		decoder := json.NewDecoder(bytes.NewReader(vector.Record))
		decoder.UseNumber()
		if err := decoder.Decode(&payload); err != nil {
			t.Fatal(err)
		}
		payload["run_id"] = "contract-test"
		payload["dataset_id"] = "contract-test"
		payload["output_id"] = vector.ExpectedOutputID
		payload["transport_measurement_version"] = 3
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := canonicalBaseLine(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if string(canonical) != vector.ExpectedCanonicalLine {
			t.Fatalf("%s canonical line mismatch\ngot:  %q\nwant: %q",
				vector.Name, canonical, vector.ExpectedCanonicalLine)
		}
		digest := sha256.Sum256(canonical)
		if got := fmt.Sprintf("%x", digest); got != vector.ExpectedOutputID {
			t.Fatalf("%s output ID = %s, want %s", vector.Name, got, vector.ExpectedOutputID)
		}
	}
}

func TestCommitLatencyHistogramAndProgressAreDeterministic(t *testing.T) {
	summary := Summary{DatabaseTransactionCommit: newDurationHistogram()}
	summary.DatabaseTransactionCommit.observe(3 * time.Millisecond)
	summary.DatabaseTransactionCommit.observe(30 * time.Millisecond)
	if summary.DatabaseTransactionCommit.Count != 2 ||
		summary.DatabaseTransactionCommit.Buckets["le_5_ms"] != 1 ||
		summary.DatabaseTransactionCommit.Buckets["le_50_ms"] != 2 {
		t.Fatalf("unexpected histogram: %+v", summary.DatabaseTransactionCommit)
	}
	path := filepath.Join(t.TempDir(), "materializer-progress.json")
	if err := saveSummary(path, summary); err != nil {
		t.Fatal(err)
	}
}

func TestConfigAcceptsRunSpecificTopics(t *testing.T) {
	t.Setenv("MATERIALIZER_BROKERS", "kafka-1:9092,kafka-2:9092")
	t.Setenv("MATERIALIZER_DATABASE_URL", "postgres://taxi@database/taxi")
	t.Setenv("MATERIALIZER_GROUP_ID", "run-1-materializer")
	t.Setenv("MATERIALIZER_QUERY1_TOPIC", "taxi.run-1.query1")
	t.Setenv("MATERIALIZER_QUERY2_TOPIC", "taxi.run-1.query2")
	config, err := ConfigFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.Query1Topic != "taxi.run-1.query1" || config.Query2Topic != "taxi.run-1.query2" {
		t.Fatalf("unexpected run topics: %#v", config)
	}
	if config.BatchSize != 1000 || config.FlushInterval != 100*time.Millisecond {
		t.Fatalf("unexpected batching defaults: %#v", config)
	}
}

func TestRecordBatchWaitsForSizeOrTimeout(t *testing.T) {
	start := time.Unix(100, 0)
	batch := newRecordBatch(3, 100*time.Millisecond)
	batch.add([]*kgo.Record{{Offset: 1}}, start)
	if batch.ready(start.Add(99 * time.Millisecond)) {
		t.Fatal("partial batch flushed before its timeout")
	}
	batch.add([]*kgo.Record{{Offset: 2}, {Offset: 3}}, start.Add(50*time.Millisecond))
	if !batch.ready(start.Add(50*time.Millisecond)) || len(batch.take()) != 3 {
		t.Fatal("full batch did not flush")
	}
	batch.add([]*kgo.Record{{Offset: 4}}, start)
	if !batch.ready(start.Add(100 * time.Millisecond)) {
		t.Fatal("partial batch did not flush at its timeout")
	}
}

func TestTransportV3DecodesBoundaryOperatorTimestamps(t *testing.T) {
	full := make([]byte, 112)
	full[0], full[1], full[2] = 3, 1, 0
	binary.BigEndian.PutUint64(full[8:16], 7)
	for index, value := range []uint64{100, 200, 250, 260, 270, 280, 300, 400, 500, 510} {
		binary.BigEndian.PutUint64(full[16+index*8:24+index*8], value)
	}
	binary.BigEndian.PutUint32(full[96:100], 10)
	binary.BigEndian.PutUint32(full[100:104], 5)
	binary.BigEndian.PutUint32(full[104:108], 25)
	record := &kgo.Record{Headers: []kgo.RecordHeader{{Key: "transport_trace_v3", Value: full}}}
	measurement, err := decodeSerializationMeasurement(record, 3)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(measurement.Trace, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["partition_boundary_emitted_epoch_us"] != float64(270) ||
		decoded["shared_boundary_emitted_epoch_us"] != float64(280) ||
		decoded["transport_measurement_version"] != float64(3) {
		t.Fatalf("unexpected Transport v3 trace: %s", measurement.Trace)
	}
	if measurement.StartedEpochMicros == nil || *measurement.StartedEpochMicros != 515 ||
		measurement.CompletedEpochMicros == nil || *measurement.CompletedEpochMicros != 540 {
		t.Fatalf("unexpected Transport v3 serializer times: %+v", measurement)
	}
}

func TestKafkaHeaderBytesIncludesKeysAndValues(t *testing.T) {
	headers := []kgo.RecordHeader{
		{Key: "a", Value: []byte("12")},
		{Key: "bc", Value: []byte("345")},
	}
	if kafkaHeaderBytes(headers) != 8 {
		t.Fatalf("unexpected Kafka header bytes: %d", kafkaHeaderBytes(headers))
	}
}

func TestBatchSizePercentiles(t *testing.T) {
	var sizes batchSizeSummary
	for _, size := range []int{10, 50, 100, 1000} {
		sizes.observe(size)
	}
	if sizes.Average != 290 || sizes.P50 != 50 || sizes.P95 != 1000 || sizes.P99 != 1000 {
		t.Fatalf("unexpected batch summary: %+v", sizes)
	}
}

func TestDatabaseBatchRetryStopsAfterSuccess(t *testing.T) {
	attempts := 0
	result, retries, err := retryCommitBatch(
		context.Background(), 3, time.Nanosecond,
		func() (batchWriteResult, error) {
			attempts++
			if attempts < 3 {
				return batchWriteResult{}, errors.New("temporary database failure")
			}
			return batchWriteResult{Inserted: 7}, nil
		})
	if err != nil || retries != 2 || result.Inserted != 7 {
		t.Fatalf("unexpected retry result: result=%+v retries=%d error=%v", result, retries, err)
	}
}

func validResultPayload(t *testing.T, query string, terminal bool) []byte {
	t.Helper()
	top := []map[string]any{}
	if query == "query1" {
		top = append(top, map[string]any{
			"count":                 1,
			"dropoff_cell":          "2.2",
			"freshness_source_line": 1,
			"freshness_time":        "2013-01-01T00:00:00Z",
			"pickup_cell":           "1.1",
			"route_id":              "1.1>2.2",
		})
	}
	value := map[string]any{
		"canonical_format_version": 1,
		"effective_time":           "2013-01-01T00:00:00Z",
		"grading_mode":             "event-time-state",
		"query_id":                 query,
		"terminal":                 terminal,
		"top":                      top,
		"trigger_event_id":         nil,
		"trigger_source_line":      nil,
		"trigger_type":             "event_time_timer",
		"run_id":                   "run",
	}
	withoutID, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := canonicalDigest(withoutID)
	if err != nil {
		t.Fatal(err)
	}
	value["output_id"] = digest
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
