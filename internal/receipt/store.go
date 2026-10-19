// Package receipt provides a durable, idempotent reference message handler.
package receipt

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/distributed-systerms-lab/queue-service/internal/message"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

var ErrConflict = errors.New("message ID already belongs to a different envelope")

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, address string, concurrency int) (*Store, error) {
	if address == "" {
		return nil, errors.New("DATABASE_URL is required for the reference consumer")
	}
	configuration, err := pgxpool.ParseConfig(address)
	if err != nil {
		return nil, errors.New("DATABASE_URL is invalid")
	}
	configuration.MaxConns = int32(concurrency + 2)
	pool, err := pgxpool.NewWithConfig(ctx, configuration)
	if err != nil {
		return nil, errors.New("create PostgreSQL connection pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("connect to PostgreSQL: check address, credentials and reachability")
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Migrate is intentionally small for the lab. The transaction-scoped advisory
// lock serializes first-time startup when several consumer processes start.
func (s *Store) Migrate(ctx context.Context) error {
	transaction, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin schema migration: %w", err)
	}
	defer transaction.Rollback(context.Background())
	if _, err := transaction.Exec(ctx, "SELECT pg_advisory_xact_lock(7264358101)"); err != nil {
		return fmt.Errorf("lock schema migration: %w", err)
	}
	if _, err := transaction.Exec(ctx, schema); err != nil {
		return fmt.Errorf("create receipt schema: %w", err)
	}
	return transaction.Commit(ctx)
}

// Record atomically inserts the handler's effect. The primary key arbitrates
// concurrent deliveries across goroutines, processes and service replicas.
// The returned boolean is false for an already processed identical message.
func (s *Store) Record(ctx context.Context, queue string, item message.Message) (bool, error) {
	fingerprint := item.Fingerprint()
	payload := item.Payload
	if payload == nil {
		payload = []byte{}
	}
	result, err := s.pool.Exec(ctx, `
		INSERT INTO processed_messages
		    (queue_name, message_id, fingerprint, message_type, payload, correlation_id, causation_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (queue_name, message_id) DO NOTHING`,
		queue, item.ID, fingerprint[:], item.Type, payload, item.CorrelationID, item.CausationID)
	if err != nil {
		return false, fmt.Errorf("record processed message: %w", err)
	}
	if result.RowsAffected() == 1 {
		return true, nil
	}
	var stored []byte
	if err := s.pool.QueryRow(ctx, `SELECT fingerprint FROM processed_messages WHERE queue_name = $1 AND message_id = $2`, queue, item.ID).Scan(&stored); err != nil {
		return false, fmt.Errorf("read existing receipt: %w", err)
	}
	if !bytes.Equal(stored, fingerprint[:]) {
		return false, ErrConflict
	}
	return false, nil
}
