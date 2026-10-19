package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	queuev1 "github.com/distributed-systerms-lab/queue-service/gen/queue/v1"
	"github.com/distributed-systerms-lab/queue-service/internal/broker"
	"github.com/distributed-systerms-lab/queue-service/internal/message"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type publisherFunc func(context.Context, string, message.Message) error

func (f publisherFunc) Publish(ctx context.Context, queue string, item message.Message) error {
	return f(ctx, queue, item)
}

func TestPublishMapsFailures(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure error
		code    codes.Code
	}{
		{"accepted", nil, codes.OK},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"canceled", context.Canceled, codes.Canceled},
		{"rejected", broker.ErrRejected, codes.ResourceExhausted},
		{"unroutable", broker.ErrUnroutable, codes.Unavailable},
		{"offline", errors.New("offline"), codes.Unavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			publisher := publisherFunc(func(ctx context.Context, queue string, item message.Message) error {
				if _, exists := ctx.Deadline(); !exists {
					t.Error("publisher has no deadline")
				}
				if queue != "tasks" || item.ID != "example" || string(item.Payload) != "hello" || item.CorrelationID != "run" || item.CausationID != "parent" {
					t.Error("envelope changed")
				}
				return test.failure
			})
			service := New(publisher, []string{"tasks"}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
			response, err := service.Publish(t.Context(), validRequest())
			if status.Code(err) != test.code {
				t.Fatalf("code = %v, want %v", status.Code(err), test.code)
			}
			if err == nil && response.MessageId != "example" {
				t.Fatal("response lost message ID")
			}
		})
	}
}

func TestPublishRejectsInvalidRequestsBeforePublishing(t *testing.T) {
	service := New(publisherFunc(func(context.Context, string, message.Message) error {
		t.Fatal("invalid request reached broker")
		return nil
	}), []string{"tasks"}, time.Second, slog.Default())
	for _, request := range []*queuev1.PublishRequest{nil, {}, {Queue: "other", Message: validRequest().Message}, {Queue: "tasks", Message: &queuev1.Message{Type: "lab.echo"}}} {
		if _, err := service.Publish(t.Context(), request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected invalid argument, got %v", err)
		}
	}
}

func TestGRPCContractRoundTrip(t *testing.T) {
	listener := bufconn.Listen(2 << 20)
	transport := grpc.NewServer()
	queuev1.RegisterQueueServiceServer(transport, New(publisherFunc(func(context.Context, string, message.Message) error { return nil }), []string{"tasks"}, time.Second, slog.Default()))
	go func() { _ = transport.Serve(listener) }()
	t.Cleanup(transport.Stop)
	connection, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	response, err := queuev1.NewQueueServiceClient(connection).Publish(ctx, validRequest())
	if err != nil {
		t.Fatal(err)
	}
	if response.MessageId != "example" {
		t.Fatalf("unexpected response: %v", response)
	}
}

func validRequest() *queuev1.PublishRequest {
	return &queuev1.PublishRequest{Queue: "tasks", Message: &queuev1.Message{Id: "example", Type: "lab.echo", Payload: []byte("hello"), CorrelationId: "run", CausationId: "parent"}}
}
