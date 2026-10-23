package delivery

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Event struct {
	ID        string
	TargetURL string
	Payload   []byte
}

type OutboxItem struct {
	Event
	ClaimToken string
}

type ClaimResult int

const (
	Claimed ClaimResult = iota
	ClaimBusy
	ClaimTerminal
	ClaimFailed
	ClaimNotFound
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Migrate applies embedded, versioned migrations in independent transactions.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	versions := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) >= 4 {
			versions = append(versions, e.Name())
		}
	}
	sort.Strings(versions)
	for _, version := range versions {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", version, err)
		}
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(734192004)`); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("lock migration %s: %w", version, err)
		}
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists); err == nil && !exists {
			var sql []byte
			sql, err = migrationFiles.ReadFile("migrations/" + version)
			if err == nil {
				_, err = tx.Exec(ctx, string(sql))
			}
			if err == nil {
				_, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, version)
			}
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", version, err)
		}
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", version, err)
		}
	}
	return nil
}

// Accept atomically stores the event, delivery state, and outbox record.
func (s *Store) Accept(ctx context.Context, ev Event) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin event transaction: %w", err)
	}
	defer tx.Rollback(context.Background())
	var inserted string
	err = tx.QueryRow(ctx, `INSERT INTO webhook_events(id,target_url,payload) VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING RETURNING id`, ev.ID, ev.TargetURL, ev.Payload).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		var same bool
		if err = tx.QueryRow(ctx, `SELECT target_url=$2 AND payload=$3 FROM webhook_events WHERE id=$1`, ev.ID, ev.TargetURL, ev.Payload).Scan(&same); err != nil {
			return false, fmt.Errorf("check duplicate event: %w", err)
		}
		if !same {
			return false, ErrIDConflict
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("insert event: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO deliveries(event_id) VALUES($1)`, ev.ID); err != nil {
		return false, fmt.Errorf("insert delivery: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO delivery_outbox(event_id) VALUES($1)`, ev.ID); err != nil {
		return false, fmt.Errorf("insert outbox: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit event: %w", err)
	}
	return true, nil
}

var ErrIDConflict = errors.New("event ID already exists with different content")

func (s *Store) ClaimOutbox(ctx context.Context, limit int, lease time.Duration) ([]OutboxItem, error) {
	if limit < 1 {
		return nil, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `WITH picked AS (
	 SELECT o.event_id FROM delivery_outbox o
	 WHERE o.published_at IS NULL AND (o.claim_until IS NULL OR o.claim_until < now())
	 ORDER BY o.created_at FOR UPDATE SKIP LOCKED LIMIT $1
	), claimed AS (
	 UPDATE delivery_outbox o SET claim_token=gen_random_uuid(), claim_until=now()+$2 * interval '1 microsecond'
	 FROM picked p WHERE o.event_id=p.event_id
	 RETURNING o.event_id,o.claim_token
	) SELECT e.id,e.target_url,e.payload,c.claim_token FROM claimed c JOIN webhook_events e ON e.id=c.event_id`, limit, lease.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	defer rows.Close()
	items := make([]OutboxItem, 0)
	for rows.Next() {
		var item OutboxItem
		if err := rows.Scan(&item.ID, &item.TargetURL, &item.Payload, &item.ClaimToken); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) MarkPublished(ctx context.Context, id, token string) error {
	ct, err := s.pool.Exec(ctx, `UPDATE delivery_outbox SET published_at=now(),claim_token=NULL,claim_until=NULL WHERE event_id=$1 AND claim_token=$2 AND published_at IS NULL`, id, token)
	if err != nil {
		return err
	}
	if ct.RowsAffected() != 1 {
		return fmt.Errorf("outbox claim lost for event %s", id)
	}
	return nil
}
func (s *Store) ReleaseOutbox(ctx context.Context, id, token string) error {
	_, err := s.pool.Exec(ctx, `UPDATE delivery_outbox SET claim_until=now(),claim_token=NULL WHERE event_id=$1 AND claim_token=$2 AND published_at IS NULL`, id, token)
	return err
}

func (s *Store) ClaimDelivery(ctx context.Context, id string, lease time.Duration) (ClaimResult, int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `UPDATE deliveries SET state='processing',claim_until=now()+$2 * interval '1 microsecond',updated_at=now() WHERE event_id=$1 AND (state='pending' OR (state='processing' AND claim_until<now())) AND (claim_until IS NULL OR claim_until<now()) RETURNING attempt_count`, id, lease.Microseconds()).Scan(&count)
	if err == nil {
		return Claimed, count, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, err
	}
	var state string
	err = s.pool.QueryRow(ctx, `SELECT state FROM deliveries WHERE event_id=$1`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return ClaimNotFound, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if state == "failed" {
		return ClaimFailed, 0, nil
	}
	if state == "delivered" {
		return ClaimTerminal, 0, nil
	}
	return ClaimBusy, 0, nil
}
func (s *Store) RecordAttempt(ctx context.Context, id string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `UPDATE deliveries SET attempt_count=attempt_count+1,updated_at=now() WHERE event_id=$1 AND state='processing' RETURNING attempt_count`, id).Scan(&n)
	return n, err
}
func (s *Store) RecordAttemptError(ctx context.Context, id, msg string, final bool) error {
	state := "processing"
	if final {
		state = "failed"
	}
	ct, err := s.pool.Exec(ctx, `UPDATE deliveries SET state=$2,last_error=$3,claim_until=CASE WHEN $4 THEN NULL ELSE claim_until END,updated_at=now() WHERE event_id=$1 AND state='processing'`, id, state, msg, final)
	if err != nil {
		return err
	}
	if ct.RowsAffected() != 1 {
		return fmt.Errorf("delivery %s is not processing", id)
	}
	return nil
}
func (s *Store) MarkDelivered(ctx context.Context, id string) error {
	ct, err := s.pool.Exec(ctx, `UPDATE deliveries SET state='delivered',last_error=NULL,delivered_at=now(),claim_until=NULL,updated_at=now() WHERE event_id=$1 AND state='processing'`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() != 1 {
		return fmt.Errorf("delivery %s is not processing", id)
	}
	return nil
}
func (s *Store) ReleaseDelivery(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE deliveries SET state='pending',claim_until=NULL,updated_at=now() WHERE event_id=$1 AND state='processing'`, id)
	return err
}
func (s *Store) GetDeliveryState(ctx context.Context, id string) (string, error) {
	var state string
	err := s.pool.QueryRow(ctx, `SELECT state FROM deliveries WHERE event_id=$1`, id).Scan(&state)
	return state, err
}
