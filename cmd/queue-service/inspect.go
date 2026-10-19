package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"time"

	"github.com/distributed-systerms-lab/queue-service/internal/broker"
	"github.com/distributed-systerms-lab/queue-service/internal/message"
)

func inspect(ctx context.Context, command string, arguments []string) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	queue := flags.String("queue", "tasks", "source queue name")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if err := message.ValidateQueue(*queue); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	connection, err := broker.Dial(ctx, os.Getenv("AMQP_URL"), 5*time.Second)
	if err != nil {
		return err
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	encoder := json.NewEncoder(os.Stdout)
	if command == "peek-dead" {
		delivery, found, err := channel.Get(*queue+".dead", false)
		if err != nil {
			return err
		}
		if !found {
			return encoder.Encode(map[string]bool{"found": false})
		}
		encodingError := encoder.Encode(struct {
			Found         bool   `json:"found"`
			ID            string `json:"message_id"`
			Type          string `json:"type"`
			Payload       []byte `json:"payload"`
			CorrelationID string `json:"correlation_id"`
			Headers       any    `json:"headers"`
		}{true, delivery.MessageId, delivery.Type, delivery.Body, delivery.CorrelationId, delivery.Headers})
		return errors.Join(encodingError, delivery.Nack(false, true))
	}
	for _, name := range []string{*queue, *queue + ".dead"} {
		state, err := channel.QueueInspect(name)
		if err != nil {
			return err
		}
		if err := encoder.Encode(struct {
			Name      string `json:"queue"`
			Ready     int    `json:"messages_ready"`
			Consumers int    `json:"consumers"`
		}{state.Name, state.Messages, state.Consumers}); err != nil {
			return err
		}
	}
	return nil
}
