package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/razvan4974/forgequeue/internal/jobs"
)

type Worker struct {
	store *jobs.Store
}

func New(store *jobs.Store) *Worker {
	return &Worker{store: store}
}

func (w *Worker) RunOnce(ctx context.Context) error {
	job, found, err := w.store.ClaimNextQueuedJob(ctx)

	if err != nil {
		return err
	}

	if !found {
		log.Println("no queueed jobs found")
		return nil
	}

	log.Printf("claimed job id=%d type=%s attempts=%d", job.ID, job.Type, job.Attempts)

	if err := w.processJob(ctx, job); err != nil {
		if markErr := w.store.MarkJobFailed(ctx, job.ID, err.Error()); markErr != nil {
			return fmt.Errorf("process job failed: %w; also failed to mark job failed: %v", err, markErr)
		}

		return err
	}

	if err := w.store.MarkJobSucceded(ctx, job.ID); err != nil {
		return err
	}

	log.Printf("completed job id=%d", job.ID)

	return nil
}

func (w *Worker) processJob(ctx context.Context, job jobs.WorkerJob) error {
	log.Printf("processing job id=%d type=%s attempts=%d", job.ID, job.Type, job.Attempts)

	select {
	case <-time.After(1 * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
