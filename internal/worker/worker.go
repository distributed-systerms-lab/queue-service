// Package worker processes messages with manual acknowledgments and bounded retries.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/distributed-systerms-lab/queue-service/internal/config"
	"github.com/distributed-systerms-lab/queue-service/internal/message"
	"github.com/distributed-systerms-lab/queue-service/internal/receipt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Recorder interface {
	Record(context.Context, string, message.Message) (bool, error)
}

type Worker struct {
	connection *amqp.Connection
	recorder   Recorder
	config     config.Config
	logger     *slog.Logger
}

func New(connection *amqp.Connection, recorder Recorder, configuration config.Config, logger *slog.Logger) *Worker {
	return &Worker{connection: connection, recorder: recorder, config: configuration, logger: logger}
}

// Run creates one channel and one prefetched delivery per worker. No delivery
// is shared across workers; total in-flight work is bounded by concurrency.
func (w *Worker) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var group sync.WaitGroup
	failures := make(chan error, w.config.Concurrency)
	for index := 0; index < w.config.Concurrency; index++ {
		group.Go(func() {
			if err := w.consume(ctx); err != nil {
				failures <- err
				cancel()
			}
		})
	}
	group.Wait()
	select {
	case err := <-failures:
		return err
	default:
		return nil
	}
}

func (w *Worker) consume(ctx context.Context) error {
	channel, err := w.connection.Channel()
	if err != nil {
		return fmt.Errorf("open consuming channel: %w", err)
	}
	defer channel.Close()
	if err := channel.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set consumer prefetch: %w", err)
	}
	deliveries, err := channel.Consume(w.config.Queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("start consumer: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, open := <-deliveries:
			if !open {
				if ctx.Err() != nil {
					return nil
				}
				return errors.New("RabbitMQ delivery stream closed")
			}
			if ctx.Err() != nil {
				return nil
			}
			if err := w.handle(ctx, delivery); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

func (w *Worker) handle(ctx context.Context, delivery amqp.Delivery) error {
	item := decode(delivery)
	retries := deliveryCount(delivery.Headers)
	logger := w.logger.With("message_id", item.ID, "queue", w.config.Queue, "type", item.Type,
		"correlation_id", item.CorrelationID, "causation_id", item.CausationID,
		"redelivered", delivery.Redelivered, "retry_count", retries)
	if err := item.Validate(); err != nil {
		logger.Warn("invalid message dead-lettered", "error", err)
		return delivery.Nack(false, false)
	}
	if err := wait(ctx, w.config.ProcessingDelay); err != nil {
		return err
	}
	var inserted bool
	var processingError error
	if item.Type == "lab.fail" {
		processingError = errors.New("requested failure injection")
	} else {
		operation, cancel := context.WithTimeout(ctx, w.config.OperationTimeout)
		inserted, processingError = w.recorder.Record(operation, w.config.Queue, item)
		cancel()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if processingError != nil {
		if errors.Is(processingError, receipt.ErrConflict) || retries >= int64(w.config.MaxRetries) {
			logger.Warn("message dead-lettered", "error", processingError)
			return delivery.Nack(false, false)
		}
		logger.Warn("message will be retried", "error", processingError)
		// Hold this worker's unacknowledged slot during backoff. No duplicate
		// publish/ack handoff is needed: the broker retains the original message.
		if err := wait(ctx, w.config.RetryDelay); err != nil {
			return err
		}
		// basic.reject counts as a failed delivery on RabbitMQ 4.3+, whereas
		// basic.nack with requeue=true no longer increments delivery-count.
		return delivery.Reject(true)
	}
	logger.Info("message processed", "new_receipt", inserted)
	// A deliberate fault-injection window after the durable effect, before ACK.
	if err := wait(ctx, w.config.AcknowledgmentDelay); err != nil {
		return err
	}
	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("acknowledge message: %w", err)
	}
	logger.Info("message acknowledged")
	return nil
}

func decode(delivery amqp.Delivery) message.Message {
	causation, _ := delivery.Headers["causation-id"].(string)
	return message.Message{ID: delivery.MessageId, Type: delivery.Type, Payload: delivery.Body,
		CorrelationID: delivery.CorrelationId, CausationID: causation}
}

func deliveryCount(headers amqp.Table) int64 {
	switch count := headers["x-delivery-count"].(type) {
	case int64:
		return count
	case int32:
		return int64(count)
	case int:
		return int64(count)
	default:
		return 0
	}
}

func wait(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if duration == 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
