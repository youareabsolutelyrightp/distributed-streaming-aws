package materialize

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Brokers       []string
	DatabaseURL   string
	GroupID       string
	Query1Topic   string
	Query2Topic   string
	BatchSize     int
	FlushInterval time.Duration
	RetryAttempts int
	RetryBackoff  time.Duration
	ExitAfterIdle time.Duration
	ProgressPath  string
}

func ConfigFromEnvironment() (Config, error) {
	config := Config{
		Brokers:       split(os.Getenv("MATERIALIZER_BROKERS")),
		DatabaseURL:   os.Getenv("MATERIALIZER_DATABASE_URL"),
		GroupID:       os.Getenv("MATERIALIZER_GROUP_ID"),
		Query1Topic:   text(os.Getenv("MATERIALIZER_QUERY1_TOPIC"), "taxi.top-routes.v1"),
		Query2Topic:   text(os.Getenv("MATERIALIZER_QUERY2_TOPIC"), "taxi.profitable-areas.v1"),
		BatchSize:     integer(os.Getenv("MATERIALIZER_BATCH_SIZE"), 1000),
		FlushInterval: duration(os.Getenv("MATERIALIZER_FLUSH_INTERVAL"), 100*time.Millisecond),
		RetryAttempts: integer(os.Getenv("MATERIALIZER_RETRY_ATTEMPTS"), 3),
		RetryBackoff:  duration(os.Getenv("MATERIALIZER_RETRY_BACKOFF"), 100*time.Millisecond),
		ExitAfterIdle: duration(os.Getenv("MATERIALIZER_EXIT_AFTER_IDLE"), 0),
		ProgressPath:  strings.TrimSpace(os.Getenv("MATERIALIZER_PROGRESS_PATH")),
	}
	if len(config.Brokers) == 0 || config.DatabaseURL == "" || config.GroupID == "" ||
		config.Query1Topic == "" || config.Query2Topic == "" {
		return Config{}, errors.New("MATERIALIZER_BROKERS, MATERIALIZER_DATABASE_URL, and MATERIALIZER_GROUP_ID are required")
	}
	if config.BatchSize <= 0 || config.FlushInterval <= 0 ||
		config.RetryAttempts <= 0 || config.RetryBackoff <= 0 {
		return Config{}, errors.New("materializer batch, flush, and retry settings must be positive")
	}
	return config, nil
}

func text(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func split(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}

func integer(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return -1
	}
	return parsed
}

func duration(value string, fallback time.Duration) time.Duration {
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return -1
	}
	return parsed
}
