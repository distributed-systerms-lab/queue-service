// Package broker implements RabbitMQ topology, publishing and consumption.
package broker

import (
	"context"
	"errors"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Dial bounds initial connection setup and socket writes. The AMQP client's
// PublishWithContext does not itself interrupt a blocked socket write.
func Dial(ctx context.Context, address string, timeout time.Duration) (*amqp.Connection, error) {
	if address == "" {
		return nil, errors.New("AMQP_URL is required")
	}
	connection, err := amqp.DialConfig(address, amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Dial: func(network, address string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: timeout}
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return &boundedConnection{Conn: conn, timeout: timeout}, nil
		},
	})
	if err != nil {
		// Do not include the URI: it contains credentials.
		return nil, errors.New("connect to RabbitMQ: check address, credentials and reachability")
	}
	return connection, nil
}

type boundedConnection struct {
	net.Conn
	timeout time.Duration
}

func (c *boundedConnection) Write(data []byte) (int, error) {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(data)
}
