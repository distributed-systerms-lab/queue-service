// Package server adapts the gRPC contract to the message publisher.
package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	queuev1 "github.com/distributed-systerms-lab/queue-service/gen/queue/v1"
	"github.com/distributed-systerms-lab/queue-service/internal/broker"
	"github.com/distributed-systerms-lab/queue-service/internal/message"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Publisher interface {
	Publish(context.Context, string, message.Message) error
}

type Server struct {
	queuev1.UnimplementedQueueServiceServer
	publisher Publisher
	queues    map[string]struct{}
	timeout   time.Duration
	logger    *slog.Logger
}

func New(publisher Publisher, queues []string, timeout time.Duration, logger *slog.Logger) *Server {
	allowed := make(map[string]struct{}, len(queues))
	for _, name := range queues {
		allowed[name] = struct{}{}
	}
	return &Server{publisher: publisher, queues: allowed, timeout: timeout, logger: logger}
}

func (s *Server) Publish(ctx context.Context, request *queuev1.PublishRequest) (*queuev1.PublishResponse, error) {
	if request == nil || request.Message == nil {
		return nil, status.Error(codes.InvalidArgument, "message is required")
	}
	if _, exists := s.queues[request.Queue]; !exists {
		return nil, status.Error(codes.InvalidArgument, "queue is not configured")
	}
	item := message.Message{ID: request.Message.Id, Type: request.Message.Type,
		Payload: request.Message.Payload, CorrelationID: request.Message.CorrelationId, CausationID: request.Message.CausationId}
	if err := item.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if err := s.publisher.Publish(ctx, request.Queue, item); err != nil {
		s.logger.Warn("publication failed", "queue", request.Queue, "message_id", item.ID, "error", err)
		switch {
		case errors.Is(err, context.Canceled):
			return nil, status.Error(codes.Canceled, "publication canceled; outcome may be unknown")
		case errors.Is(err, context.DeadlineExceeded):
			return nil, status.Error(codes.DeadlineExceeded, "broker confirmation timed out; retry with the same message ID")
		case errors.Is(err, broker.ErrRejected):
			return nil, status.Error(codes.ResourceExhausted, "broker rejected the message")
		default:
			return nil, status.Error(codes.Unavailable, "broker unavailable; retry with the same message ID")
		}
	}
	s.logger.Info("message accepted", "queue", request.Queue, "message_id", item.ID,
		"correlation_id", item.CorrelationID, "causation_id", item.CausationID)
	return &queuev1.PublishResponse{MessageId: item.ID}, nil
}
