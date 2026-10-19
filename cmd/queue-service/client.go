package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	queuev1 "github.com/distributed-systerms-lab/queue-service/gen/queue/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

func publish(ctx context.Context, arguments []string, logger *slog.Logger) error {
	flags := flag.NewFlagSet("publish", flag.ContinueOnError)
	address := flags.String("address", "queue-service:50051", "gRPC server address")
	queue := flags.String("queue", "tasks", "configured queue name")
	id := flags.String("id", "", "message ID; generated when omitted")
	kind := flags.String("type", "lab.echo", "message type (lab.fail injects processing failure)")
	payload := flags.String("payload", "hello", "message payload")
	correlation := flags.String("correlation-id", "", "correlation ID")
	causation := flags.String("causation-id", "", "causation ID")
	timeout := flags.Duration("timeout", 10*time.Second, "request deadline")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || *timeout <= 0 {
		return errors.New("unexpected positional arguments or non-positive timeout")
	}
	if *id == "" {
		*id = rand.Text()
	}
	logger.Info("publishing message; reuse this ID if the outcome is uncertain", "message_id", *id)
	connection, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create gRPC client: %w", err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	response, err := queuev1.NewQueueServiceClient(connection).Publish(ctx, &queuev1.PublishRequest{
		Queue:   *queue,
		Message: &queuev1.Message{Id: *id, Type: *kind, Payload: []byte(*payload), CorrelationId: *correlation, CausationId: *causation},
	})
	if err != nil {
		return fmt.Errorf("publish message %s: %w", *id, err)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]string{"message_id": response.MessageId})
}

func checkHealth(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("health", flag.ContinueOnError)
	address := flags.String("address", "127.0.0.1:50051", "gRPC server address")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	connection, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	response, err := healthv1.NewHealthClient(connection).Check(ctx, &healthv1.HealthCheckRequest{Service: "queue.v1.QueueService"})
	if err != nil {
		return err
	}
	if response.Status != healthv1.HealthCheckResponse_SERVING {
		return errors.New("queue service is not serving")
	}
	return nil
}
