package store

import (
	"context"
	"database/sql"

	_ "github.com/lib/pq"
)

type AuditRecord struct {
	EventType  string
	WorkflowID string
	RunID      string
	Payload    []byte
	Partition  int
	Offset     int64
}

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// nullable maps an empty id to SQL NULL so text columns stay clean.
func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func (s *Store) AppendAudit(ctx context.Context, r AuditRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO audit_log (event_type, workflow_id, run_id, payload, kafka_partition, kafka_offset)
		 VALUES ($1, $2, $3, $4::jsonb, $5, $6)`,
		r.EventType, nullable(r.WorkflowID), nullable(r.RunID), string(r.Payload), r.Partition, r.Offset)
	return err
}

func (s *Store) UpsertCount(ctx context.Context, workflowID string, count int) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO workflow_run_counts (workflow_id, completed_count, updated_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (workflow_id) DO UPDATE
		   SET completed_count = EXCLUDED.completed_count, updated_at = now()`,
		workflowID, count)
	return err
}
