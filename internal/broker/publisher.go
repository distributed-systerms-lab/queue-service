package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/distributed-systerms-lab/queue-service/internal/message"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrUnroutable = errors.New("message could not be routed")
	ErrRejected   = errors.New("broker rejected the message")
)

type Publisher struct {
	connection *amqp.Connection
	slots      chan struct{}
}

func NewPublisher(connection *amqp.Connection, concurrency int) *Publisher {
	return &Publisher{connection: connection, slots: make(chan struct{}, concurrency)}
}

// Publish uses a bounded number of independent channels. A confirmation can
// never be mistaken for another request's confirmation, even after a timeout.
// This deliberately favors a small implementation over channel pooling.
func (p *Publisher) Publish(ctx context.Context, queue string, item message.Message) error {
	select {
	case p.slots <- struct{}{}:
		defer func() { <-p.slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	channel, err := p.connection.Channel()
	if err != nil {
		return fmt.Errorf("open publishing channel: %w", err)
	}
	defer channel.Close()
	returns := channel.NotifyReturn(make(chan amqp.Return, 1))
	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	confirmation, err := channel.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		ContentType:   "application/octet-stream",
		MessageId:     item.ID,
		Type:          item.Type,
		Body:          item.Payload,
		CorrelationId: item.CorrelationID,
		Headers:       amqp.Table{"causation-id": item.CausationID},
		Timestamp:     time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("publish message: %w", err)
	}
	accepted, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait for broker confirmation: %w", err)
	}
	// RabbitMQ sends basic.return before basic.ack for mandatory unroutable
	// messages. The client dispatches both frames on the same channel reader.
	select {
	case <-returns:
		return ErrUnroutable
	default:
	}
	if !accepted {
		return ErrRejected
	}
	return nil
}
