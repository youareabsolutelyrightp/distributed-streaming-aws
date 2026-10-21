package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Query1VerifyConfig struct {
	Brokers             []string
	RunID               string
	Query1Topic         string
	Query1Partitions    int
	ControlTopic        string
	DLQTopic            string
	DLQPartitions       int
	ReferenceReportPath string
	ReportPath          string
	OutputLinesPath     string
	Timeout             time.Duration
	QuietPeriod         time.Duration
}

type Query1VerificationReport struct {
	Version                 int    `json:"version"`
	RunID                   string `json:"run_id"`
	GradingMode             string `json:"grading_mode"`
	ExpectedRecords         uint64 `json:"expected_records"`
	ObservedRecords         uint64 `json:"observed_records"`
	RawOutputRecords        uint64 `json:"raw_output_records"`
	DuplicateOutputs        uint64 `json:"duplicate_outputs"`
	ExtraUniqueOutputs      uint64 `json:"extra_unique_outputs"`
	ExpectedDigest          string `json:"expected_digest"`
	ObservedDigest          string `json:"observed_digest"`
	OutputIDsVerified       bool   `json:"output_ids_verified"`
	TerminalResultSeen      bool   `json:"terminal_result_seen"`
	ProducerCompleteSeen    bool   `json:"producer_complete_seen"`
	ZeroConsumerLagObserved bool   `json:"zero_consumer_lag_observed"`
	QuietPeriodObserved     bool   `json:"quiet_period_observed"`
	LateEvents              uint64 `json:"late_events"`
	LateCorrectnessFailures uint64 `json:"late_correctness_failures"`
	PostTerminalOutputs     uint64 `json:"post_terminal_outputs"`
	CanonicalMatch          bool   `json:"canonical_match"`
	Passed                  bool   `json:"passed"`
}

type query1ReferenceReport struct {
	Streams map[string]struct {
		Digest  string `json:"digest"`
		Records uint64 `json:"records"`
	} `json:"streams"`
}

var query1CanonicalKeys = []string{
	"canonical_format_version",
	"effective_time",
	"grading_mode",
	"query_id",
	"terminal",
	"top",
	"trigger_event_id",
	"trigger_source_line",
	"trigger_type",
}

func VerifyQuery1(ctx context.Context, config Query1VerifyConfig) (*Query1VerificationReport, error) {
	if len(config.Brokers) == 0 || config.RunID == "" || config.ReferenceReportPath == "" {
		return nil, errors.New("brokers, run-id, and reference-report are required")
	}
	if config.Timeout <= 0 {
		config.Timeout = 60 * time.Second
	}
	if config.QuietPeriod <= 0 {
		config.QuietPeriod = time.Second
	}
	query1Topic := config.Query1Topic
	if query1Topic == "" {
		query1Topic = Query1Topic
	}
	query1Partitions := config.Query1Partitions
	if query1Partitions == 0 {
		query1Partitions = ResultPartitions
	}
	if query1Partitions != ResultPartitions {
		return nil, fmt.Errorf("Query 1 result topic must have %d partition", ResultPartitions)
	}
	controlTopic := config.ControlTopic
	if controlTopic == "" {
		controlTopic = ControlTopic
	}
	dlqTopic := config.DLQTopic
	if dlqTopic == "" {
		dlqTopic = DLQTopic
	}
	dlqPartitions := config.DLQPartitions
	if dlqPartitions == 0 {
		dlqPartitions = 4
	}
	stream := "query1.event-time-state"
	referenceBytes, err := os.ReadFile(config.ReferenceReportPath)
	if err != nil {
		return nil, fmt.Errorf("read Query 1 reference report: %w", err)
	}
	var reference query1ReferenceReport
	if err := json.Unmarshal(referenceBytes, &reference); err != nil {
		return nil, fmt.Errorf("decode Query 1 reference report: %w", err)
	}
	expected, exists := reference.Streams[stream]
	if !exists || expected.Records == 0 || len(expected.Digest) != 64 {
		return nil, fmt.Errorf("reference report has no valid %s stream", stream)
	}
	report := &Query1VerificationReport{
		Version: 1, RunID: config.RunID, GradingMode: BenchmarkGradingMode,
		ExpectedRecords: expected.Records, ExpectedDigest: expected.Digest,
		OutputIDsVerified: true,
	}
	digest := sha256.New()
	observedLines, err := newAtomicOutput(config.OutputLinesPath)
	if err != nil {
		return report, fmt.Errorf("create observed Query 1 output: %w", err)
	}
	defer observedLines.Abort()
	seenOutputIDs := newOutputIDSet(expected.Records)
	consumeErr := consumeAssignedUntilQuiet(
		ctx,
		config.Brokers,
		map[string]int{query1Topic: query1Partitions, dlqTopic: dlqPartitions, controlTopic: 1},
		config.Timeout,
		config.QuietPeriod,
		func() bool { return report.ProducerCompleteSeen && report.TerminalResultSeen },
		func(record *kgo.Record) error {
			if record.Topic == controlTopic {
				var control map[string]any
				if err := json.Unmarshal(record.Value, &control); err != nil {
					return fmt.Errorf("decode run control: %w", err)
				}
				if control["run_id"] == config.RunID && control["record_type"] == "producer_complete" {
					report.ProducerCompleteSeen = true
				}
				return nil
			}
			if record.Topic == dlqTopic {
				var rejected map[string]any
				if err := json.Unmarshal(record.Value, &rejected); err != nil {
					return fmt.Errorf("decode DLQ record: %w", err)
				}
				if rejected["run_id"] == config.RunID && rejected["reason"] == "late_event" {
					report.LateEvents++
					if failure, _ := rejected["correctness_failure"].(bool); failure {
						report.LateCorrectnessFailures++
					}
				}
				return nil
			}
			decoder := json.NewDecoder(bytes.NewReader(record.Value))
			decoder.UseNumber()
			var value map[string]any
			if err := decoder.Decode(&value); err != nil {
				return fmt.Errorf("decode Query 1 output: %w", err)
			}
			if value["run_id"] != config.RunID || value["grading_mode"] != BenchmarkGradingMode {
				return nil
			}
			report.RawOutputRecords++
			terminalAlreadySeen := report.TerminalResultSeen
			canonicalBase := make(map[string]any, len(query1CanonicalKeys))
			for _, key := range query1CanonicalKeys {
				field, present := value[key]
				if !present {
					return fmt.Errorf("Query 1 output is missing canonical field %s", key)
				}
				canonicalBase[key] = field
			}
			baseBytes, err := canonicalJSONLine(canonicalBase)
			if err != nil {
				return err
			}
			computedID := sha256.Sum256(baseBytes)
			outputID, ok := value["output_id"].(string)
			if !ok || outputID != hex.EncodeToString(computedID[:]) {
				report.OutputIDsVerified = false
				return fmt.Errorf("Query 1 output_id does not match its canonical value")
			}
			if seenOutputIDs.Remember(computedID) {
				report.DuplicateOutputs++
				return nil
			}
			if terminalAlreadySeen {
				report.PostTerminalOutputs++
			}
			canonicalBase["output_id"] = outputID
			finalBytes, err := canonicalJSONLine(canonicalBase)
			if err != nil {
				return err
			}
			_, _ = digest.Write(finalBytes)
			if err := observedLines.Write(finalBytes); err != nil {
				return fmt.Errorf("write observed Query 1 output: %w", err)
			}
			report.ObservedRecords++
			if terminal, ok := value["terminal"].(bool); ok && terminal {
				report.TerminalResultSeen = true
			}
			if report.ObservedRecords > report.ExpectedRecords {
				report.ExtraUniqueOutputs++
			}
			return nil
		})
	if err := observedLines.Commit(); err != nil {
		return report, fmt.Errorf("commit observed Query 1 output: %w", err)
	}
	if consumeErr != nil {
		if config.ReportPath != "" {
			_ = saveJSONAtomic(config.ReportPath, report)
		}
		return report, consumeErr
	}
	report.ZeroConsumerLagObserved = true
	report.QuietPeriodObserved = true
	report.ObservedDigest = hex.EncodeToString(digest.Sum(nil))
	report.CanonicalMatch = report.ObservedRecords == report.ExpectedRecords && report.ObservedDigest == report.ExpectedDigest
	report.Passed = report.CanonicalMatch &&
		report.OutputIDsVerified &&
		report.TerminalResultSeen &&
		report.ProducerCompleteSeen &&
		report.ZeroConsumerLagObserved &&
		report.QuietPeriodObserved &&
		report.LateCorrectnessFailures == 0 &&
		report.PostTerminalOutputs == 0
	if config.ReportPath != "" {
		if err := saveJSONAtomic(config.ReportPath, report); err != nil {
			return report, err
		}
	}
	if !report.Passed {
		return report, errors.New("Query 1 canonical verification failed")
	}
	return report, nil
}

func canonicalJSONLine(value any) ([]byte, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return canonicalUTF8Line(encoded.Bytes()), nil
}

func canonicalUTF8Line(encoded []byte) []byte {
	result := make([]byte, 0, len(encoded))
	for index := 0; index < len(encoded); {
		if encoded[index] != '\\' {
			result = append(result, encoded[index])
			index++
			continue
		}
		start := index
		for index < len(encoded) && encoded[index] == '\\' {
			index++
		}
		count := index - start
		if count%2 == 1 && index+5 <= len(encoded) && encoded[index] == 'u' {
			code := string(encoded[index : index+5])
			if code == "u2028" || code == "u2029" {
				result = append(result, encoded[start:index-1]...)
				if code == "u2028" {
					result = append(result, 0xe2, 0x80, 0xa8)
				} else {
					result = append(result, 0xe2, 0x80, 0xa9)
				}
				index += 5
				continue
			}
		}
		result = append(result, encoded[start:index]...)
	}
	return result
}
