package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/razvan4974/forgequeue/internal/jobs"
)

const pollInterval = 500 * time.Millisecond

type Worker struct {
	store *jobs.Store
}

func New(store *jobs.Store) *Worker {
	return &Worker{store: store}
}

func (w *Worker) Run(ctx context.Context, concurrency int) {
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)

		go func(workerNum int) {
			defer wg.Done()
			w.loop(ctx, workerNum)
		}(i + 1)
	}
	wg.Wait()
}

func (w *Worker) loop(ctx context.Context, workerNum int) {
	// idle tracks whether this goroutine has already reported an empty queue, so
	// the message is logged once per idle period instead of once per poll.
	idle := false

	for {
		if ctx.Err() != nil {
			return
		}

		found, err := w.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			log.Printf("worker %d: run once failed: %v", workerNum, err)

			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}

			continue
		}

		if found {
			idle = false
			continue
		}

		if !idle {
			log.Printf("worker %d: queue empty, waiting", workerNum)
			idle = true
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	job, found, err := w.store.ClaimNextQueuedJob(ctx)

	if err != nil {
		return false, err
	}

	if !found {
		return false, nil
	}

	log.Printf("claimed job id=%d type=%s attempts=%d", job.ID, job.Type, job.Attempts)

	if err := w.processJob(ctx, job); err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}

		if markErr := w.store.MarkJobFailed(ctx, job.ID, err.Error()); markErr != nil {
			return true, fmt.Errorf("process job failed: %w; also failed to mark job failed: %v", err, markErr)
		}

		return true, err
	}

	if err := w.store.MarkJobSucceeded(ctx, job.ID); err != nil {
		return true, err
	}

	log.Printf("completed job id=%d", job.ID)

	return true, nil
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
