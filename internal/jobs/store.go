package jobs

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CreateJobRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Job struct {
	ID     int64  `json:"id"`
	Status string `json:"status"`
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) CreateJob(ctx context.Context, req CreateJobRequest) (Job, error) {
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}

	var job Job

	query := `
		INSERT INTO jobs (type, payload, status)
		VALUES ($1, $2, 'queued')
		RETURNING id, status; 
	`

	err := s.db.QueryRow(
		ctx,
		query,
		req.Type,
		req.Payload,
	).Scan(&job.ID, &job.Status)

	if err != nil {
		return Job{}, err
	}

	return job, nil
}

type WorkerJob struct {
	ID       int64           `json:"id"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Status   string          `json:"status"`
	Attempts int             `json:"attempts"`
}

func (s *Store) ClaimNextQueuedJob(ctx context.Context) (WorkerJob, bool, error) {
	query := `
		WITH next_job AS (
			SELECT id 
			FROM jobs
			WHERE status = 'queued'
			ORDER BY created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs
		SET status = 'running',
			attempts = attempts + 1,
			started_at = NOW(), 
			updated_at = NOW()
		FROM next_job
		WHERE jobs.id = next_job.id
		RETURNING jobs.id, jobs.type, jobs.payload, jobs.status, jobs.attempts;
	`

	var job WorkerJob

	err := s.db.QueryRow(ctx, query).Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.Status,
		&job.Attempts,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return WorkerJob{}, false, nil
	}

	if err != nil {
		return WorkerJob{}, false, err
	}

	return job, true, nil
}

func (s *Store) MarkJobSucceeded(ctx context.Context, id int64) error {
	query := `
		UPDATE jobs
		SET status = 'succeeded', 
			completed_at = NOW(), 
			updated_at = NOW()
		WHERE id = $1
	`

	_, err := s.db.Exec(ctx, query, id)
	return err
}

func (s *Store) MarkJobFailed(ctx context.Context, id int64, errorMessage string) error {
	query := `
		UPDATE jobs
		SET status = 'failed',
			failed_at = NOW(),
			error_message = $2,
			updated_at = NOW()
		WHERE id = $1;
	`

	_, err := s.db.Exec(ctx, query, id, errorMessage)
	return err
}
