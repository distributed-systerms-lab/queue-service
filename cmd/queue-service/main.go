// Command queue-service runs the gRPC API, the reference consumer or a lab client.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/distributed-systerms-lab/queue-service/internal/broker"
	"github.com/distributed-systerms-lab/queue-service/internal/config"
	"github.com/distributed-systerms-lab/queue-service/internal/receipt"
	"github.com/distributed-systerms-lab/queue-service/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], logger)
	stop()
	if err != nil {
		logger.Error("command failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string, logger *slog.Logger) error {
	if len(arguments) == 0 {
		return errors.New("usage: queue-service <serve|worker|publish|health|inspect|peek-dead>")
	}
	switch arguments[0] {
	case "publish":
		return publish(ctx, arguments[1:], logger)
	case "health":
		return checkHealth(ctx, arguments[1:])
	case "inspect", "peek-dead":
		return inspect(ctx, arguments[0], arguments[1:])
	case "serve", "worker":
		if len(arguments) != 1 {
			return errors.New("serve and worker accept configuration through environment variables")
		}
		configuration, err := config.Load()
		if err != nil {
			return err
		}
		startup, cancel := context.WithTimeout(ctx, configuration.OperationTimeout)
		connection, err := broker.Dial(startup, configuration.AMQPURL, configuration.OperationTimeout)
		cancel()
		if err != nil {
			return err
		}
		defer connection.Close()
		if err := broker.Declare(connection, configuration.Queues, configuration.MaxRetries, configuration.MaxQueueLength); err != nil {
			return err
		}
		if arguments[0] == "serve" {
			return serve(ctx, configuration, connection, logger)
		}
		startup, cancel = context.WithTimeout(ctx, configuration.OperationTimeout)
		store, err := receipt.Open(startup, configuration.DatabaseURL, configuration.Concurrency)
		if err == nil {
			err = store.Migrate(startup)
		}
		cancel()
		if err != nil {
			if store != nil {
				store.Close()
			}
			return err
		}
		defer store.Close()
		logger.Info("consumer started", "queue", configuration.Queue, "concurrency", configuration.Concurrency)
		return worker.New(connection, store, configuration, logger).Run(ctx)
	default:
		return fmt.Errorf("unknown command %q", arguments[0])
	}
}
