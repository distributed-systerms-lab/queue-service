package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Declare(connection *amqp.Connection, queues []string, maxRetries, maxLength int) error {
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open topology channel: %w", err)
	}
	defer channel.Close()
	for _, name := range queues {
		if _, err := channel.QueueDeclare(name+".dead", true, false, false, false, amqp.Table{
			"x-queue-type": "quorum",
			// Dead letters are inspected manually and must survive repeated peeks.
			"x-delivery-limit": int32(-1),
		}); err != nil {
			return fmt.Errorf("declare dead-letter queue %s: %w", name, err)
		}
		if _, err := channel.QueueDeclare(name, true, false, false, false, amqp.Table{
			"x-queue-type":              "quorum",
			"x-delivery-limit":          int32(maxRetries),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": name + ".dead",
			"x-dead-letter-strategy":    "at-least-once",
			"x-overflow":                "reject-publish",
			"x-max-length":              int32(maxLength),
		}); err != nil {
			return fmt.Errorf("declare queue %s: %w", name, err)
		}
	}
	return nil
}
