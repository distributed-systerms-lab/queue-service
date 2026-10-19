// Package config parses and validates process configuration at startup.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/distributed-systerms-lab/queue-service/internal/message"
)

type Config struct {
	AMQPURL             string
	ListenAddress       string
	Queues              []string
	Queue               string
	DatabaseURL         string
	Concurrency         int
	MaxRetries          int
	MaxQueueLength      int
	PublishConcurrency  int
	OperationTimeout    time.Duration
	ShutdownTimeout     time.Duration
	RetryDelay          time.Duration
	ProcessingDelay     time.Duration
	AcknowledgmentDelay time.Duration
}

func Load() (Config, error) {
	configuration := Config{
		AMQPURL:       os.Getenv("AMQP_URL"),
		ListenAddress: value("GRPC_ADDRESS", ":50051"),
		Queue:         value("QUEUE_NAME", "tasks"),
		DatabaseURL:   os.Getenv("DATABASE_URL"),
	}
	seen := make(map[string]bool)
	for _, name := range strings.Split(value("QUEUES", "tasks"), ",") {
		name = strings.TrimSpace(name)
		if err := message.ValidateQueue(name); err != nil {
			return Config{}, fmt.Errorf("QUEUES: %w", err)
		}
		if !seen[name] {
			configuration.Queues = append(configuration.Queues, name)
			seen[name] = true
		}
	}
	if !seen[configuration.Queue] {
		return Config{}, fmt.Errorf("QUEUE_NAME %q is not in QUEUES", configuration.Queue)
	}
	for _, field := range []struct {
		name                       string
		target                     *int
		fallback, minimum, maximum int
	}{
		{"CONCURRENCY", &configuration.Concurrency, 4, 1, 128},
		{"MAX_RETRIES", &configuration.MaxRetries, 3, 0, 100},
		{"MAX_QUEUE_LENGTH", &configuration.MaxQueueLength, 100000, 1, 10000000},
		{"PUBLISH_CONCURRENCY", &configuration.PublishConcurrency, 32, 1, 256},
	} {
		number, err := strconv.Atoi(value(field.name, strconv.Itoa(field.fallback)))
		if err != nil || number < field.minimum || number > field.maximum {
			return Config{}, fmt.Errorf("%s must be an integer between %d and %d", field.name, field.minimum, field.maximum)
		}
		*field.target = number
	}
	for _, field := range []struct {
		name      string
		target    *time.Duration
		fallback  string
		allowZero bool
	}{
		{"OPERATION_TIMEOUT", &configuration.OperationTimeout, "5s", false},
		{"SHUTDOWN_TIMEOUT", &configuration.ShutdownTimeout, "15s", false},
		{"RETRY_DELAY", &configuration.RetryDelay, "1s", true},
		{"PROCESSING_DELAY", &configuration.ProcessingDelay, "0s", true},
		{"ACKNOWLEDGMENT_DELAY", &configuration.AcknowledgmentDelay, "0s", true},
	} {
		duration, err := time.ParseDuration(value(field.name, field.fallback))
		if err != nil || duration < 0 || (!field.allowZero && duration == 0) || duration > time.Minute {
			minimum := "greater than zero"
			if field.allowZero {
				minimum = "at least zero"
			}
			return Config{}, fmt.Errorf("%s must be a duration %s and at most 1m", field.name, minimum)
		}
		*field.target = duration
	}
	return configuration, nil
}

func value(name, fallback string) string {
	if setting, exists := os.LookupEnv(name); exists {
		return setting
	}
	return fallback
}
