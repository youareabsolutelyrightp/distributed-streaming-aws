package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestPreparedSourcePreservesIdentityAndDecimalLexemes(t *testing.T) {
	path := writePreparedRow(t, "-73.956528")
	source, err := OpenPrepared(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	event, err := source.Read("run", 123)
	if err != nil {
		t.Fatal(err)
	}
	if event.SourceLineNumber != 1 ||
		string(event.PickupLongitude) != "-73.956528" ||
		event.EventID != ExpectedEventID("dataset", "joined-1") {
		t.Fatalf("unexpected prepared event: %+v", event)
	}
}

func TestPreparedSourceRejectsNonCanonicalCoordinate(t *testing.T) {
	path := writePreparedRow(t, "-7.3956528e1")
	source, err := OpenPrepared(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Read("run", 123); err == nil {
		t.Fatal("exponent coordinate was accepted")
	}
}

func TestStablePartitionVectors(t *testing.T) {
	var document struct {
		PartitioningVersion string `json:"partitioning_version"`
		HashVectorVersion   string `json:"hash_test_vector_version"`
		Vectors             []struct {
			Medallion         string `json:"medallion"`
			PartitionCount    int    `json:"partition_count"`
			ExpectedPartition int32  `json:"expected_partition"`
		} `json:"vectors"`
	}
	readContractVector(t, "partitioning-test-vectors.json", &document)
	if document.PartitioningVersion != PartitioningVersion ||
		document.HashVectorVersion != PartitionHashVectorVersion {
		t.Fatalf("partition contract identity changed: %+v", document)
	}
	for _, vector := range document.Vectors {
		observed, err := PartitionForMedallion(vector.Medallion, vector.PartitionCount)
		if err != nil {
			t.Fatal(err)
		}
		if observed != vector.ExpectedPartition {
			t.Fatalf("partition for %q/%d = %d, want %d",
				vector.Medallion, vector.PartitionCount, observed, vector.ExpectedPartition)
		}
	}
}

func TestCanonicalGoldenVectors(t *testing.T) {
	data := readContractBytes(t, "canonical-test-vectors.jsonl")
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
		decoder := json.NewDecoder(bytes.NewReader(vector.Record))
		decoder.UseNumber()
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		canonical, err := canonicalJSONLine(record)
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

func readContractVector(t *testing.T, name string, output any) {
	t.Helper()
	if err := json.Unmarshal(readContractBytes(t, name), output); err != nil {
		t.Fatal(err)
	}
}

func readContractBytes(t *testing.T, name string) []byte {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate replay contract tests")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(source), "..", "..", "schemas", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestFrozenProducerAndPartitionContract(t *testing.T) {
	manifest := Manifest{PreparedEventCount: 100}
	config := Config{
		Brokers: []string{"kafka:9092"}, RunID: "run", Rate: 20_000,
		CheckpointPath: "checkpoint", SummaryPath: "summary",
	}
	if err := config.normalize(manifest); err != nil {
		t.Fatal(err)
	}
	if BenchmarkProfile != "controlled" || BenchmarkGradingMode != "event-time-state" {
		t.Fatal("frozen producer identity changed")
	}
	if topic, partitions := config.inputTopicAndPartitions(); topic != DistributedTopic || partitions != 4 {
		t.Fatalf("input contract = %s/%d", topic, partitions)
	}
}

func TestVersionFourCheckpointAllowsOnlyRateChange(t *testing.T) {
	manifest := Manifest{
		DatasetID: "dataset", PreparedChecksum: "checksum", PreparedEventCount: 100,
	}
	config := Config{
		RunID: "run",
		Rate:  20_000, Limit: 100, InputTopic: "taxi.run.trips", InputPartitions: 4,
	}
	checkpoint := Checkpoint{
		Version: 4, RunID: config.RunID, DatasetID: manifest.DatasetID,
		ReplayConfigSHA256:         replayConfigDigest(config, manifest),
		PreparedChecksum:           manifest.PreparedChecksum,
		PartitioningVersion:        PartitioningVersion,
		PartitionHashVectorVersion: PartitionHashVectorVersion,
		InputPartitionCount:        4, LastContiguousAcknowledged: 40,
	}
	changedRate := config
	changedRate.Rate = 50_000
	if err := validateCheckpoint(checkpoint, changedRate, manifest); err != nil {
		t.Fatalf("rate-only resume failed: %v", err)
	}
	changedTopic := changedRate
	changedTopic.InputTopic = "taxi.other.trips"
	if err := validateCheckpoint(checkpoint, changedTopic, manifest); err == nil {
		t.Fatal("immutable topic change was accepted")
	}
}

func TestProgressRecorderWritesAcknowledgementState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")
	recorder, err := startProgressRecorder(path, func() progressRecord {
		return progressRecord{
			Phase: "measurement", AcknowledgedSourceRecords: 123,
			PhaseAcknowledgedRecords: 23, CurrentRequestedRate: 50_000,
			TopicPartitions: map[string]PartitionSummary{
				"taxi.trips/0": {Acknowledged: 60, LastOffset: 59},
			},
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record progressRecord
	if err := json.Unmarshal(bytes.TrimSpace(data), &record); err != nil {
		t.Fatal(err)
	}
	if record.Phase != "measurement" || record.AcknowledgedSourceRecords != 123 ||
		record.TopicPartitions["taxi.trips/0"].Acknowledged != 60 {
		t.Fatalf("unexpected progress record: %+v", record)
	}
}

func TestAckTrackerAdvancesOnlyAcrossContiguousRows(t *testing.T) {
	tracker := newAckTracker(10)
	if got := tracker.acknowledge(12); got != 10 {
		t.Fatalf("got %d, want 10", got)
	}
	if got := tracker.acknowledge(11); got != 12 {
		t.Fatalf("got %d, want 12", got)
	}
}

func TestLatencyHistogramAndStableControlID(t *testing.T) {
	histogram := newLatencyHistogram()
	histogram.observe(3 * time.Millisecond)
	if histogram.Buckets["le_5_ms"] != 1 || histogram.Buckets["le_10_ms"] != 1 {
		t.Fatalf("histogram is not cumulative: %#v", histogram.Buckets)
	}
	if stableControlID("run", "run_start") != stableControlID("run", "run_start") {
		t.Fatal("control ID is not deterministic")
	}
}

func TestProduceSyncJSONRejectsNegativePartition(t *testing.T) {
	err := produceSyncJSON(context.Background(), nil, ControlTopic, []byte("run"), -1, struct{}{})
	if err == nil || !strings.Contains(err.Error(), "partition must be non-negative") {
		t.Fatalf("negative partition was not rejected: %v", err)
	}
}

func writePreparedRow(t *testing.T, coordinate string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.csv.zst")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := zstd.NewWriter(file)
	if err != nil {
		t.Fatal(err)
	}
	header := append([]string(nil), preparedColumns...)
	values := map[string]string{
		"schema_version": "1", "dataset_id": "dataset",
		"event_id": ExpectedEventID("dataset", "joined-1"), "source_line_number": "1",
		"month_id":             "1",
		"trip_source_checksum": strings.Repeat("b", 64), "trip_original_row_number": "1",
		"fare_source_checksum": strings.Repeat("c", 64), "fare_original_row_number": "1",
		"joined_record_id": "joined-1", "medallion": "taxi-a", "hack_license": "driver-a",
		"vendor_id": "VTS", "pickup_datetime": "2013-01-01T00:00:00Z",
		"dropoff_datetime": "2013-01-01T00:01:00Z", "rate_code": "1",
		"passenger_count": "1", "trip_time_seconds": "60", "trip_distance_miles": "1.0",
		"pickup_longitude": coordinate, "pickup_latitude": "40.7",
		"dropoff_longitude": "-73.9", "dropoff_latitude": "40.8",
		"payment_type": "CSH", "fare_amount": "1.00", "surcharge": "0.00",
		"mta_tax": "0.50", "tip_amount": "0.00", "tolls_amount": "0.00",
		"total_amount": "1.50",
	}
	row := make([]string, len(header))
	for index, name := range header {
		row[index] = values[name]
	}
	writer := csv.NewWriter(encoder)
	if err := writer.Write(header); err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(row); err != nil {
		t.Fatal(err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	encoder.Close()
	file.Close()
	return path
}
