//go:build integration

// Package integration tests real RabbitMQ and PostgreSQL, including process death.
package integration

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	queuev1 "github.com/distributed-systerms-lab/queue-service/gen/queue/v1"
	"github.com/distributed-systerms-lab/queue-service/internal/broker"
	"github.com/distributed-systerms-lab/queue-service/internal/config"
	"github.com/distributed-systerms-lab/queue-service/internal/message"
	"github.com/distributed-systerms-lab/queue-service/internal/receipt"
	"github.com/distributed-systerms-lab/queue-service/internal/server"
	"github.com/distributed-systerms-lab/queue-service/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type environment struct {
	connection    *amqp.Connection
	store         *receipt.Store
	database      *pgxpool.Pool
	configuration config.Config
	logger        *slog.Logger
}

func setup(t *testing.T) *environment {
	t.Helper()
	if os.Getenv("AMQP_URL") == "" || os.Getenv("DATABASE_URL") == "" {
		t.Fatal("integration tests require AMQP_URL and DATABASE_URL; run make integration-up and make integration")
	}
	queue := "test-" + rand.Text()
	configuration := config.Config{AMQPURL: os.Getenv("AMQP_URL"), DatabaseURL: os.Getenv("DATABASE_URL"),
		Queue: queue, Queues: []string{queue}, Concurrency: 4, MaxRetries: 2, MaxQueueLength: 10000,
		OperationTimeout: 5 * time.Second, RetryDelay: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	connection, err := broker.Dial(ctx, configuration.AMQPURL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := broker.Declare(connection, configuration.Queues, configuration.MaxRetries, configuration.MaxQueueLength); err != nil {
		t.Fatal(err)
	}
	store, err := receipt.Open(ctx, configuration.DatabaseURL, configuration.Concurrency)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	database, err := pgxpool.New(ctx, configuration.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(database.Close)
	t.Cleanup(func() {
		channel, err := connection.Channel()
		if err == nil {
			defer channel.Close()
			_, _ = channel.QueueDelete(queue, false, false, false)
			_, _ = channel.QueueDelete(queue+".dead", false, false, false)
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = database.Exec(cleanup, "DELETE FROM processed_messages WHERE queue_name=$1", queue)
	})
	return &environment{connection: connection, store: store, database: database, configuration: configuration, logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}
}

func (e *environment) startWorker(t *testing.T, recorder worker.Recorder) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- worker.New(e.connection, recorder, e.configuration, e.logger).Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("consumer failed: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("consumer did not stop")
		}
	})
}

func (e *environment) client(t *testing.T) queuev1.QueueServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transport := grpc.NewServer()
	queuev1.RegisterQueueServiceServer(transport, server.New(broker.NewPublisher(e.connection, 8), e.configuration.Queues, 5*time.Second, e.logger))
	go func() { _ = transport.Serve(listener) }()
	t.Cleanup(transport.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return queuev1.NewQueueServiceClient(connection)
}

func (e *environment) receiptCount(t *testing.T, id string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var count int
	if err := e.database.QueryRow(ctx, "SELECT count(*) FROM processed_messages WHERE queue_name=$1 AND message_id=$2", e.configuration.Queue, id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestPublishConsumeAndConcurrentDuplicates(t *testing.T) {
	e := setup(t)
	recorder := &countingRecorder{store: e.store}
	e.startWorker(t, recorder)
	client := e.client(t)
	request := &queuev1.PublishRequest{Queue: e.configuration.Queue, Message: &queuev1.Message{Id: "same-message", Type: "lab.echo", Payload: []byte("hello"), CorrelationId: "test-run", CausationId: "test-parent"}}
	const deliveries = 24
	var group sync.WaitGroup
	failures := make(chan error, deliveries)
	for index := 0; index < deliveries; index++ {
		group.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			_, err := client.Publish(ctx, request)
			failures <- err
		})
	}
	group.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool { return recorder.deliveries.Load() == deliveries })
	if count := e.receiptCount(t, request.Message.Id); count != 1 {
		t.Fatalf("receipt count=%d, want 1", count)
	}
	if recorder.insertions.Load() != 1 {
		t.Fatalf("new effects=%d, want 1", recorder.insertions.Load())
	}
	var kind, correlation, causation string
	var payload []byte
	if err := e.database.QueryRow(t.Context(), `SELECT message_type,payload,correlation_id,causation_id FROM processed_messages WHERE queue_name=$1 AND message_id=$2`, e.configuration.Queue, request.Message.Id).Scan(&kind, &payload, &correlation, &causation); err != nil {
		t.Fatal(err)
	}
	if kind != "lab.echo" || string(payload) != "hello" || correlation != "test-run" || causation != "test-parent" {
		t.Fatal("message envelope was not preserved")
	}
}

type countingRecorder struct {
	store      *receipt.Store
	deliveries atomic.Int64
	insertions atomic.Int64
}

func (r *countingRecorder) Record(ctx context.Context, queue string, item message.Message) (bool, error) {
	inserted, err := r.store.Record(ctx, queue, item)
	if inserted {
		r.insertions.Add(1)
	}
	r.deliveries.Add(1)
	return inserted, err
}

func TestFailedMessageReachesDeadLetterQueue(t *testing.T) {
	e := setup(t)
	e.startWorker(t, e.store)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := broker.NewPublisher(e.connection, 1).Publish(ctx, e.configuration.Queue, message.Message{ID: "poison-message", Type: "lab.fail"}); err != nil {
		t.Fatal(err)
	}
	channel, err := e.connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	var dead amqp.Delivery
	eventually(t, func() bool {
		delivery, ok, err := channel.Get(e.configuration.Queue+".dead", false)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			dead = delivery
		}
		return ok
	})
	if dead.MessageId != "poison-message" || dead.Type != "lab.fail" {
		t.Fatalf("unexpected dead letter: %+v", dead)
	}
	if dead.Headers["x-death"] == nil {
		t.Fatal("dead letter has no broker death metadata")
	}
	if err := dead.Ack(false); err != nil {
		t.Fatal(err)
	}
	if e.receiptCount(t, "poison-message") != 0 {
		t.Fatal("failed message produced an effect")
	}
}

func TestUnroutablePublicationIsNotAccepted(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := broker.NewPublisher(e.connection, 1).Publish(ctx, "missing-"+rand.Text(), message.Message{ID: "unroutable", Type: "lab.echo"})
	if !errors.Is(err, broker.ErrUnroutable) {
		t.Fatalf("Publish()=%v, want ErrUnroutable", err)
	}
}

func TestConflictingMessageIDIsRejected(t *testing.T) {
	e := setup(t)
	item := message.Message{ID: "conflict", Type: "lab.echo", Payload: []byte("first")}
	if _, err := e.store.Record(t.Context(), e.configuration.Queue, item); err != nil {
		t.Fatal(err)
	}
	item.Payload = []byte("different")
	if _, err := e.store.Record(t.Context(), e.configuration.Queue, item); !errors.Is(err, receipt.ErrConflict) {
		t.Fatalf("Record()=%v, want ErrConflict", err)
	}
}

func TestConsumerCrashAfterCommitDoesNotDuplicateEffect(t *testing.T) {
	e := setup(t)
	first, firstLog := e.startProcess(t, "30s")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := broker.NewPublisher(e.connection, 1).Publish(ctx, e.configuration.Queue, message.Message{ID: "crash-window", Type: "lab.echo", Payload: []byte("survives-redelivery")}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return e.receiptCount(t, "crash-window") == 1 && contains(firstLog, `"msg":"message processed"`)
	})
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = first.Wait()
	_, replacementLog := e.startProcess(t, "0s")
	eventually(t, func() bool { return contains(replacementLog, `"msg":"message acknowledged"`) })
	if !contains(replacementLog, `"redelivered":true`) || !contains(replacementLog, `"new_receipt":false`) {
		t.Fatalf("replacement did not suppress a redelivery: %s", readLog(replacementLog))
	}
	if e.receiptCount(t, "crash-window") != 1 {
		t.Fatal("crash produced a duplicate effect")
	}
}

func (e *environment) startProcess(t *testing.T, acknowledgmentDelay string) (*exec.Cmd, string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "consumer.log")
	output, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = output.Close() })
	command := exec.Command(executable, "-test.run=^TestConsumerProcess$")
	command.Env = append(os.Environ(), "QUEUE_TEST_HELPER=1", "QUEUE_NAME="+e.configuration.Queue, "QUEUES="+e.configuration.Queue, "CONCURRENCY=1", "MAX_RETRIES=2", "MAX_QUEUE_LENGTH=10000", "ACKNOWLEDGMENT_DELAY="+acknowledgmentDelay)
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	return command, path
}

// TestConsumerProcess becomes a standalone worker in a child test process.
func TestConsumerProcess(t *testing.T) {
	if os.Getenv("QUEUE_TEST_HELPER") != "1" {
		return
	}
	c, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	connection, err := broker.Dial(ctx, c.AMQPURL, c.OperationTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := broker.Declare(connection, c.Queues, c.MaxRetries, c.MaxQueueLength); err != nil {
		t.Fatal(err)
	}
	store, err := receipt.Open(ctx, c.DatabaseURL, c.Concurrency)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := worker.New(connection, store, c, logger).Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}

func readLog(path string) string      { data, _ := os.ReadFile(path); return string(data) }
func contains(path, text string) bool { return strings.Contains(readLog(path), text) }
