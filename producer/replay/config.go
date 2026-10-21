package replay

import (
	"errors"
	"fmt"
	"strings"
)

const (
	BenchmarkProfile     = "controlled"
	BenchmarkGradingMode = "event-time-state"
	DistributedTopic     = "taxi.trips.v1"
	ControlTopic         = "taxi.run-control.v1"
	DLQTopic             = "taxi.dlq.v1"
	Query1Topic          = "taxi.top-routes.v1"
	Query2Topic          = "taxi.profitable-areas.v1"
	ResultPartitions     = 1
)

type Config struct {
	Brokers          []string
	ManifestPath     string
	InputPath        string
	RunID            string
	InputTopic       string
	InputPartitions  int
	ControlTopic     string
	DLQTopic         string
	Rate             float64
	Limit            uint64
	Resume           bool
	CheckpointPath   string
	SummaryPath      string
	ProgressPath     string
	ProgressPhase    string
	PauseAfterRecord uint64
	MaximumEventTime string
	CheckpointEvery  uint64
}

func (config *Config) normalize(manifest Manifest) error {
	if len(config.Brokers) == 0 {
		return errors.New("at least one Kafka broker is required")
	}
	if strings.TrimSpace(config.RunID) == "" {
		return errors.New("run-id is required")
	}
	if strings.TrimSpace(config.ControlTopic) == "" {
		config.ControlTopic = ControlTopic
	}
	if strings.TrimSpace(config.DLQTopic) == "" {
		config.DLQTopic = DLQTopic
	}
	if config.CheckpointEvery == 0 {
		config.CheckpointEvery = 1000
	}
	if config.Rate <= 0 {
		return errors.New("rate must be positive")
	}
	if config.Limit == 0 || config.Limit > manifest.PreparedEventCount {
		config.Limit = manifest.PreparedEventCount
	}
	if config.PauseAfterRecord > config.Limit {
		return errors.New("pause-after-record exceeds the replay limit")
	}
	_, partitions := config.inputTopicAndPartitions()
	if partitions <= 0 {
		return errors.New("input partitions must be positive")
	}
	inputTopic, inputPartitions := config.inputTopicAndPartitions()
	config.InputTopic = inputTopic
	config.InputPartitions = inputPartitions
	if config.CheckpointPath == "" || config.SummaryPath == "" {
		return errors.New("checkpoint and summary paths are required")
	}
	if config.ProgressPhase == "" {
		config.ProgressPhase = "replay"
	}
	switch config.ProgressPhase {
	case "replay", "warmup", "measurement", "postroll":
	default:
		return fmt.Errorf("unknown progress phase %q", config.ProgressPhase)
	}
	return nil
}

func (config Config) requestedRate() float64 {
	return config.Rate
}

func (config Config) inputTopicAndPartitions() (string, int) {
	topic := strings.TrimSpace(config.InputTopic)
	partitions := config.InputPartitions
	if topic == "" {
		topic = DistributedTopic
	}
	if partitions == 0 {
		partitions = 4
	}
	return topic, partitions
}
