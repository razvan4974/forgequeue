package jobs

import (
	"context"
	"encoding/json"

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
