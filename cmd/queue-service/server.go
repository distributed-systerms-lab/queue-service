package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	queuev1 "github.com/distributed-systerms-lab/queue-service/gen/queue/v1"
	"github.com/distributed-systerms-lab/queue-service/internal/broker"
	"github.com/distributed-systerms-lab/queue-service/internal/config"
	"github.com/distributed-systerms-lab/queue-service/internal/message"
	"github.com/distributed-systerms-lab/queue-service/internal/server"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func serve(ctx context.Context, configuration config.Config, connection *amqp.Connection, logger *slog.Logger) error {
	listener, err := net.Listen("tcp", configuration.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for gRPC: %w", err)
	}
	defer listener.Close()
	transport := grpc.NewServer(
		grpc.MaxRecvMsgSize(message.MaxPayloadSize+4096),
		grpc.MaxConcurrentStreams(128),
	)
	publisher := broker.NewPublisher(connection, configuration.PublishConcurrency)
	queuev1.RegisterQueueServiceServer(transport, server.New(publisher, configuration.Queues, configuration.OperationTimeout, logger))
	readiness := health.NewServer()
	readiness.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	readiness.SetServingStatus("queue.v1.QueueService", healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(transport, readiness)
	reflection.Register(transport)
	failures := make(chan error, 1)
	go func() { failures <- transport.Serve(listener) }()
	brokerClosed := connection.NotifyClose(make(chan *amqp.Error, 1))
	logger.Info("gRPC server started", "address", listener.Addr().String(), "queues", configuration.Queues)
	var failure error
	select {
	case <-ctx.Done():
	case failure = <-failures:
	case <-brokerClosed:
		failure = errors.New("RabbitMQ connection closed; process restart required")
	}
	readiness.Shutdown()
	stopped := make(chan struct{})
	go func() { transport.GracefulStop(); close(stopped) }()
	timer := time.NewTimer(configuration.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-stopped:
	case <-timer.C:
		transport.Stop()
		<-stopped
	}
	logger.Info("gRPC server stopped")
	return failure
}
