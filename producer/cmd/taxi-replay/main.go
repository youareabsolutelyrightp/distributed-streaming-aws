package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/reconstructed-foil/streaming-pipeline/producer/replay"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(arguments []string) int {
	if len(arguments) == 0 {
		usage()
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch arguments[0] {
	case "replay":
		return replayCommand(ctx, arguments[1:])
	case "verify-run":
		return verifyRunCommand(ctx, arguments[1:])
	case "help", "-h", "--help":
		usage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", arguments[0])
		usage()
		return 2
	}
}

func replayCommand(ctx context.Context, arguments []string) int {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	brokers := flags.String("brokers", "kafka:9092", "comma-separated Kafka brokers")
	manifest := flags.String("manifest", "", "prepared data manifest")
	input := flags.String("input", "", "prepared zstd CSV")
	runID := flags.String("run-id", "", "stable identifier for this replay")
	inputTopic := flags.String("input-topic", "", "Kafka input topic")
	inputPartitions := flags.Int("input-partitions", 4, "Kafka input partition count")
	controlTopic := flags.String("control-topic", replay.ControlTopic, "Kafka run-control topic")
	dlqTopic := flags.String("dlq-topic", replay.DLQTopic, "Kafka dead-letter topic")
	rate := flags.Float64("rate", 0, "source records per second")
	progress := flags.String("progress", "", "one-second producer progress JSONL path")
	progressPhase := flags.String("progress-phase", "measurement", "warmup or measurement")
	limit := flags.Uint64("limit", 0, "source records to replay")
	resume := flags.Bool("resume", false, "continue from the durable producer checkpoint")
	checkpoint := flags.String("checkpoint", "", "checkpoint JSON path")
	summary := flags.String("summary", "", "summary JSON path")
	pauseAfterRecord := flags.Uint64("pause-after-record", 0, "pause after this source line")
	maximumEventTime := flags.String("maximum-event-time", "", "view-specific terminal event time")
	checkpointEvery := flags.Uint64("checkpoint-every", 1000, "persist after this many acknowledgements")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	if *runID == "" || *manifest == "" || *input == "" || *checkpoint == "" || *summary == "" {
		fmt.Fprintln(os.Stderr, "--run-id, --manifest, --input, --checkpoint, and --summary are required")
		return 2
	}
	result, err := replay.Run(ctx, replay.Config{
		Brokers: splitList(*brokers), ManifestPath: *manifest, InputPath: *input,
		RunID: *runID, InputTopic: *inputTopic, InputPartitions: *inputPartitions,
		ControlTopic: *controlTopic, DLQTopic: *dlqTopic,
		Rate:  *rate,
		Limit: *limit, Resume: *resume, CheckpointPath: *checkpoint, SummaryPath: *summary,
		ProgressPath: *progress, ProgressPhase: *progressPhase,
		PauseAfterRecord: *pauseAfterRecord, MaximumEventTime: *maximumEventTime,
		CheckpointEvery: *checkpointEvery,
	})
	printJSON(result)
	if errors.Is(err, replay.ErrPaused) {
		return 75
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func verifyRunCommand(ctx context.Context, arguments []string) int {
	flags := flag.NewFlagSet("verify-run", flag.ContinueOnError)
	brokers := flags.String("brokers", "kafka:9092", "comma-separated Kafka brokers")
	runID := flags.String("run-id", "", "run identifier")
	controlTopic := flags.String("control-topic", replay.ControlTopic, "Kafka run-control topic")
	query1Topic := flags.String("query1-topic", replay.Query1Topic, "Query 1 result topic")
	query2Topic := flags.String("query2-topic", replay.Query2Topic, "Query 2 result topic")
	dlqTopic := flags.String("dlq-topic", replay.DLQTopic, "dead-letter topic")
	inputPartitions := flags.Int("input-partitions", 4, "input and DLQ partition count")
	resultPartitions := flags.Int(
		"result-partitions", replay.ResultPartitions, "partition count for each result topic")
	reference := flags.String("reference", "", "frozen both-query reference report")
	maximumEventTime := flags.String("maximum-event-time", "", "manifest maximum event time")
	output := flags.String("output", "", "directory for three verification reports")
	timeout := flags.Duration("timeout", 30*time.Minute, "maximum verification wait")
	quietPeriod := flags.Duration("quiet-period", 2*time.Second, "zero-lag quiet period")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	if *runID == "" || *reference == "" || *maximumEventTime == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "--run-id, --reference, --maximum-event-time, and --output are required")
		return 2
	}
	if *resultPartitions != replay.ResultPartitions {
		fmt.Fprintf(os.Stderr, "--result-partitions must be %d for the frozen benchmark\n",
			replay.ResultPartitions)
		return 2
	}
	if err := os.MkdirAll(*output, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	common := splitList(*brokers)
	q1, err := replay.VerifyQuery1(ctx, replay.Query1VerifyConfig{
		Brokers: common, RunID: *runID,
		Query1Topic: *query1Topic, Query1Partitions: *resultPartitions,
		ControlTopic: *controlTopic, DLQTopic: *dlqTopic, DLQPartitions: *inputPartitions,
		ReferenceReportPath: *reference, ReportPath: filepath.Join(*output, "query1-verification.json"),
		Timeout: *timeout, QuietPeriod: *quietPeriod,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	q2, err := replay.VerifyQuery2(ctx, replay.Query2VerifyConfig{
		Brokers: common, RunID: *runID,
		Query2Topic: *query2Topic, Query2Partitions: *resultPartitions,
		ControlTopic: *controlTopic, ReferenceReportPath: *reference,
		ReportPath: filepath.Join(*output, "query2-verification.json"),
		Timeout:    *timeout, QuietPeriod: *quietPeriod,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	completion, err := replay.WaitForCompletion(ctx, replay.CompletionConfig{
		Brokers: common, RunID: *runID,
		ControlTopic: *controlTopic,
		Query1Topic:  *query1Topic, Query1Partitions: *resultPartitions,
		Query2Topic: *query2Topic, Query2Partitions: *resultPartitions,
		DLQTopic: *dlqTopic, DLQPartitions: *inputPartitions,
		ExpectedPartitions: *inputPartitions, ExpectedMaximumEventTime: *maximumEventTime,
		Timeout: *timeout, QuietPeriod: *quietPeriod,
		ReportPath: filepath.Join(*output, "completion-report.json"),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	printJSON(map[string]any{"query1": q1, "query2": q2, "completion": completion})
	return 0
}

func splitList(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}

func printJSON(value any) {
	if value == nil {
		return
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}

func usage() { fmt.Fprintln(os.Stderr, "Usage: taxi-replay <replay|verify-run> [options]") }
