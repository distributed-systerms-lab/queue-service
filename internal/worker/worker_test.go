package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/distributed-systerms-lab/queue-service/internal/config"
	"github.com/distributed-systerms-lab/queue-service/internal/message"
	"github.com/distributed-systerms-lab/queue-service/internal/receipt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type recorderFunc func(context.Context, string, message.Message) (bool, error)

func (f recorderFunc) Record(ctx context.Context, queue string, item message.Message) (bool, error) {
	return f(ctx, queue, item)
}

type acknowledgment struct {
	action   string
	requeue  bool
	multiple bool
}

func (a *acknowledgment) Ack(_ uint64, multiple bool) error {
	a.action = "ack"
	a.multiple = multiple
	return nil
}
func (a *acknowledgment) Nack(_ uint64, multiple, requeue bool) error {
	a.action = "nack"
	a.multiple = multiple
	a.requeue = requeue
	return nil
}
func (a *acknowledgment) Reject(_ uint64, requeue bool) error {
	a.action = "reject"
	a.requeue = requeue
	return nil
}

func TestDeliveryOutcomes(t *testing.T) {
	for _, test := range []struct {
		name, kind string
		retries    int64
		failure    error
		inserted   bool
		action     string
		requeue    bool
	}{
		{"success", "lab.echo", 0, nil, true, "ack", false},
		{"duplicate", "lab.echo", 0, nil, false, "ack", false},
		{"retry", "lab.echo", 0, errors.New("database offline"), false, "reject", true},
		{"exhausted", "lab.fail", 3, nil, false, "nack", false},
		{"identity conflict", "lab.echo", 0, receipt.ErrConflict, false, "nack", false},
		{"invalid envelope", "", 0, nil, false, "nack", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ack := &acknowledgment{}
			consumer := New(nil, recorderFunc(func(context.Context, string, message.Message) (bool, error) { return test.inserted, test.failure }), config.Config{Queue: "tasks", MaxRetries: 3, OperationTimeout: time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			delivery := amqp.Delivery{Acknowledger: ack, MessageId: "example", Type: test.kind, Headers: amqp.Table{"x-delivery-count": test.retries}}
			if err := consumer.handle(t.Context(), delivery); err != nil {
				t.Fatal(err)
			}
			if ack.action != test.action || ack.requeue != test.requeue || ack.multiple {
				t.Fatalf("unexpected acknowledgment: %+v", ack)
			}
		})
	}
}

func TestCancellationAfterCommitLeavesDeliveryUnacknowledged(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ack := &acknowledgment{}
	consumer := New(nil, recorderFunc(func(context.Context, string, message.Message) (bool, error) { cancel(); return true, nil }), config.Config{Queue: "tasks", OperationTimeout: time.Second}, slog.Default())
	err := consumer.handle(ctx, amqp.Delivery{Acknowledger: ack, MessageId: "example", Type: "lab.echo"})
	if !errors.Is(err, context.Canceled) || ack.action != "" {
		t.Fatalf("err=%v, acknowledgment=%+v", err, ack)
	}
}

func TestBackoffCanBeCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := wait(ctx, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait() = %v", err)
	}
}
