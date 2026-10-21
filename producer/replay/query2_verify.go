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

type Query2VerifyConfig struct {
	Brokers             []string
	RunID               string
	Query2Topic         string
	Query2Partitions    int
	ControlTopic        string
	ReferenceReportPath string
	ReportPath          string
	OutputLinesPath     string
	Timeout             time.Duration
	QuietPeriod         time.Duration
}

type Query2VerificationReport struct {
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
	PostTerminalOutputs     uint64 `json:"post_terminal_outputs"`
	CanonicalMatch          bool   `json:"canonical_match"`
	Passed                  bool   `json:"passed"`
}

func VerifyQuery2(ctx context.Context, config Query2VerifyConfig) (*Query2VerificationReport, error) {
	if len(config.Brokers) == 0 || config.RunID == "" || config.ReferenceReportPath == "" {
		return nil, errors.New("brokers, run-id, and reference-report are required")
	}
	if config.Timeout <= 0 {
		config.Timeout = 60 * time.Second
	}
	if config.QuietPeriod <= 0 {
		config.QuietPeriod = time.Second
	}
	query2Topic := config.Query2Topic
	if query2Topic == "" {
		query2Topic = Query2Topic
	}
	query2Partitions := config.Query2Partitions
	if query2Partitions == 0 {
		query2Partitions = ResultPartitions
	}
	if query2Partitions != ResultPartitions {
		return nil, fmt.Errorf("Query 2 result topic must have %d partition", ResultPartitions)
	}
	controlTopic := config.ControlTopic
	if controlTopic == "" {
		controlTopic = ControlTopic
	}
	stream := "query2.event-time-state"
	referenceBytes, err := os.ReadFile(config.ReferenceReportPath)
	if err != nil {
		return nil, fmt.Errorf("read Query 2 reference report: %w", err)
	}
	var reference query1ReferenceReport
	if err := json.Unmarshal(referenceBytes, &reference); err != nil {
		return nil, fmt.Errorf("decode Query 2 reference report: %w", err)
	}
	expected, exists := reference.Streams[stream]
	if !exists || expected.Records == 0 || len(expected.Digest) != 64 {
		return nil, fmt.Errorf("reference report has no valid %s stream", stream)
	}
	report := &Query2VerificationReport{
		Version: 1, RunID: config.RunID, GradingMode: BenchmarkGradingMode,
		ExpectedRecords: expected.Records, ExpectedDigest: expected.Digest,
		OutputIDsVerified: true,
	}
	digest := sha256.New()
	observedLines, err := newAtomicOutput(config.OutputLinesPath)
	if err != nil {
		return report, fmt.Errorf("create observed Query 2 output: %w", err)
	}
	defer observedLines.Abort()
	seenOutputIDs := newOutputIDSet(expected.Records)
	consumeErr := consumeAssignedUntilQuiet(
		ctx, config.Brokers,
		map[string]int{query2Topic: query2Partitions, controlTopic: 1},
		config.Timeout, config.QuietPeriod,
		func() bool { return report.ProducerCompleteSeen && report.TerminalResultSeen },
		func(record *kgo.Record) error {
			if record.Topic == controlTopic {
				var control map[string]any
				if err := json.Unmarshal(record.Value, &control); err != nil {
					return err
				}
				if control["run_id"] == config.RunID && control["record_type"] == "producer_complete" {
					report.ProducerCompleteSeen = true
				}
				return nil
			}
			decoder := json.NewDecoder(bytes.NewReader(record.Value))
			decoder.UseNumber()
			var value map[string]any
			if err := decoder.Decode(&value); err != nil {
				return fmt.Errorf("decode Query 2 output: %w", err)
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
					return fmt.Errorf("Query 2 output is missing canonical field %s", key)
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
				return errors.New("Query 2 output_id does not match its canonical value")
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
				return fmt.Errorf("write observed Query 2 output: %w", err)
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
		return report, fmt.Errorf("commit observed Query 2 output: %w", err)
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
	report.Passed = report.CanonicalMatch && report.OutputIDsVerified && report.TerminalResultSeen &&
		report.ProducerCompleteSeen && report.ZeroConsumerLagObserved && report.QuietPeriodObserved &&
		report.PostTerminalOutputs == 0
	if config.ReportPath != "" {
		if err := saveJSONAtomic(config.ReportPath, report); err != nil {
			return report, err
		}
	}
	if !report.Passed {
		return report, errors.New("Query 2 canonical verification failed")
	}
	return report, nil
}
