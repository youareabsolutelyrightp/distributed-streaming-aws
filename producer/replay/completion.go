package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type CompletionConfig struct {
	Brokers                  []string
	RunID                    string
	ControlTopic             string
	Query1Topic              string
	Query1Partitions         int
	Query2Topic              string
	Query2Partitions         int
	DLQTopic                 string
	DLQPartitions            int
	ExpectedPartitions       int
	ExpectedMaximumEventTime string
	Timeout                  time.Duration
	QuietPeriod              time.Duration
	ReportPath               string
}

type CompletionReport struct {
	Version                      int               `json:"version"`
	RunID                        string            `json:"run_id"`
	GradingMode                  string            `json:"grading_mode"`
	ExpectedPartitions           int               `json:"expected_partitions"`
	ExpectedMaximumEventTime     string            `json:"expected_maximum_event_time"`
	ProducerCompleteSeen         bool              `json:"producer_complete_seen"`
	ProducerEOSAcknowledged      int               `json:"producer_eos_acknowledged"`
	QueryCompleteSeen            map[string]bool   `json:"query_complete_seen"`
	QueryCompleteControlIDs      map[string]string `json:"query_complete_control_ids"`
	DuplicateCompletionControls  uint64            `json:"duplicate_completion_controls"`
	Query1TerminalResultSeen     bool              `json:"query1_terminal_result_seen"`
	Query2TerminalResultSeen     bool              `json:"query2_terminal_result_seen"`
	Query1ResultRecords          uint64            `json:"query1_result_records"`
	Query2ResultRecords          uint64            `json:"query2_result_records"`
	MaximumObservedEffectiveTime string            `json:"maximum_observed_effective_time"`
	LateCorrectnessFailures      uint64            `json:"late_correctness_failures"`
	ZeroResultAndControlLag      bool              `json:"zero_result_and_control_lag"`
	QuietPeriodObserved          bool              `json:"quiet_period_observed"`
	Errors                       []string          `json:"errors"`
	Passed                       bool              `json:"passed"`
}

type completionControl struct {
	RecordType            string `json:"record_type"`
	RunID                 string `json:"run_id"`
	DatasetID             string `json:"dataset_id"`
	GradingMode           string `json:"grading_mode"`
	QueryID               string `json:"query_id"`
	ControlID             string `json:"control_id"`
	MaximumEventTime      string `json:"maximum_event_time"`
	MaximumEventTimeMilli int64  `json:"maximum_event_time_epoch_ms"`
	ExpectedPartitions    int    `json:"expected_partition_count"`
	EOSAcknowledged       int    `json:"eos_acknowledged"`
	TerminalOutputID      string `json:"terminal_output_id"`
}

type completionResult struct {
	RunID         string `json:"run_id"`
	GradingMode   string `json:"grading_mode"`
	QueryID       string `json:"query_id"`
	OutputID      string `json:"output_id"`
	EffectiveTime string `json:"effective_time"`
	Terminal      bool   `json:"terminal"`
}

func WaitForCompletion(ctx context.Context, config CompletionConfig) (*CompletionReport, error) {
	if len(config.Brokers) == 0 || config.RunID == "" || config.ExpectedPartitions <= 0 {
		return nil, errors.New("brokers, run-id, and expected-partitions are required")
	}
	expectedMaximum, err := time.Parse(time.RFC3339, config.ExpectedMaximumEventTime)
	if err != nil {
		return nil, fmt.Errorf("parse expected maximum event time: %w", err)
	}
	if config.Timeout <= 0 {
		config.Timeout = 2 * time.Minute
	}
	if config.QuietPeriod <= 0 {
		config.QuietPeriod = time.Second
	}
	controlTopic := config.ControlTopic
	if controlTopic == "" {
		controlTopic = ControlTopic
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
	dlqTopic := config.DLQTopic
	if dlqTopic == "" {
		dlqTopic = DLQTopic
	}
	dlqPartitions := config.DLQPartitions
	if dlqPartitions == 0 {
		dlqPartitions = config.ExpectedPartitions
	}
	report := &CompletionReport{
		Version:                  1,
		RunID:                    config.RunID,
		GradingMode:              BenchmarkGradingMode,
		ExpectedPartitions:       config.ExpectedPartitions,
		ExpectedMaximumEventTime: config.ExpectedMaximumEventTime,
		QueryCompleteSeen:        map[string]bool{"query1": false, "query2": false},
		QueryCompleteControlIDs:  make(map[string]string),
	}
	seenControls := make(map[string]struct{})
	terminalOutputIDs := make(map[string]string)
	var maximumObserved time.Time

	consumeErr := consumeAssignedUntilQuiet(
		ctx,
		config.Brokers,
		map[string]int{
			controlTopic: 1,
			query1Topic:  query1Partitions,
			query2Topic:  query2Partitions,
			dlqTopic:     dlqPartitions,
		},
		config.Timeout,
		config.QuietPeriod,
		func() bool {
			return report.ProducerCompleteSeen &&
				report.QueryCompleteSeen["query1"] &&
				report.QueryCompleteSeen["query2"]
		},
		func(record *kgo.Record) error {
			switch record.Topic {
			case controlTopic:
				var control completionControl
				if err := json.Unmarshal(record.Value, &control); err != nil {
					return fmt.Errorf("decode completion control: %w", err)
				}
				if control.RunID != config.RunID {
					return nil
				}
				switch control.RecordType {
				case "producer_complete":
					report.ProducerCompleteSeen = true
					report.ProducerEOSAcknowledged = control.EOSAcknowledged
					if control.ExpectedPartitions != config.ExpectedPartitions ||
						control.EOSAcknowledged != config.ExpectedPartitions ||
						control.MaximumEventTime != config.ExpectedMaximumEventTime {
						report.Errors = append(report.Errors, "producer completion metadata does not match the run")
					}
				case "query_complete":
					if control.QueryID != "query1" && control.QueryID != "query2" {
						report.Errors = append(report.Errors, "query completion has an unknown query ID")
						return nil
					}
					expectedID := stableQueryCompleteID(config.RunID, control.QueryID)
					if control.ControlID != expectedID ||
						control.GradingMode != BenchmarkGradingMode ||
						control.MaximumEventTimeMilli != expectedMaximum.UnixMilli() {
						report.Errors = append(report.Errors, "query completion identity or terminal time is invalid")
					}
					if _, duplicate := seenControls[control.ControlID]; duplicate {
						report.DuplicateCompletionControls++
					} else {
						seenControls[control.ControlID] = struct{}{}
						report.QueryCompleteSeen[control.QueryID] = true
						report.QueryCompleteControlIDs[control.QueryID] = control.ControlID
						terminalOutputIDs[control.QueryID] = control.TerminalOutputID
					}
				}
				return nil
			case dlqTopic:
				var rejected struct {
					RunID              string `json:"run_id"`
					CorrectnessFailure bool   `json:"correctness_failure"`
				}
				if err := json.Unmarshal(record.Value, &rejected); err != nil {
					return fmt.Errorf("decode DLQ record: %w", err)
				}
				if rejected.RunID == config.RunID && rejected.CorrectnessFailure {
					report.LateCorrectnessFailures++
				}
				return nil
			default:
				var result completionResult
				if err := json.Unmarshal(record.Value, &result); err != nil {
					return fmt.Errorf("decode result while waiting for completion: %w", err)
				}
				if result.RunID != config.RunID || result.GradingMode != BenchmarkGradingMode {
					return nil
				}
				effectiveTime, err := time.Parse(time.RFC3339, result.EffectiveTime)
				if err != nil {
					return fmt.Errorf("parse result effective time: %w", err)
				}
				if effectiveTime.After(expectedMaximum) {
					report.Errors = append(report.Errors, "result effective time exceeds the manifest maximum")
				}
				if effectiveTime.After(maximumObserved) {
					maximumObserved = effectiveTime
				}
				if result.QueryID == "query1" {
					report.Query1ResultRecords++
					if result.Terminal {
						report.Query1TerminalResultSeen = true
						terminalOutputIDs["query1-result"] = result.OutputID
					}
				} else if result.QueryID == "query2" {
					report.Query2ResultRecords++
					if result.Terminal {
						report.Query2TerminalResultSeen = true
						terminalOutputIDs["query2-result"] = result.OutputID
					}
				}
				return nil
			}
		})

	if !maximumObserved.IsZero() {
		report.MaximumObservedEffectiveTime = maximumObserved.UTC().Format(time.RFC3339)
	}
	if consumeErr == nil {
		report.ZeroResultAndControlLag = true
		report.QuietPeriodObserved = true
	} else {
		report.Errors = append(report.Errors, consumeErr.Error())
	}
	if !report.Query1TerminalResultSeen || !report.Query2TerminalResultSeen {
		report.Errors = append(report.Errors, "event-time-state output is missing a terminal query result")
	}
	for _, query := range []string{"query1", "query2"} {
		if terminalOutputIDs[query] == "" ||
			terminalOutputIDs[query] != terminalOutputIDs[query+"-result"] {
			report.Errors = append(report.Errors, query+" completion does not name its terminal output")
		}
	}
	report.Passed = len(report.Errors) == 0 &&
		report.ProducerCompleteSeen &&
		report.QueryCompleteSeen["query1"] &&
		report.QueryCompleteSeen["query2"] &&
		report.ProducerEOSAcknowledged == config.ExpectedPartitions &&
		report.LateCorrectnessFailures == 0 &&
		report.ZeroResultAndControlLag &&
		report.QuietPeriodObserved
	if config.ReportPath != "" {
		if err := saveJSONAtomic(config.ReportPath, report); err != nil {
			return report, err
		}
	}
	if !report.Passed {
		return report, errors.New("controlled completion verification failed")
	}
	return report, nil
}

func stableQueryCompleteID(runID, queryID string) string {
	digest := sha256.Sum256([]byte(runID + "|query_complete|" + queryID + "|" + BenchmarkGradingMode))
	return hex.EncodeToString(digest[:])
}
