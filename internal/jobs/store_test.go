package jobs_test

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/razvan4974/forgequeue/internal/jobs"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	testDatabaseURL := os.Getenv("TEST_DATABASE_URL")
	if testDatabaseURL == "" {
		log.Fatal(
			"TEST_DATABASE_URL is not set; " +
				"use a dedicated test database, for example: " +
				"postgres://forgequeue:forgequeue@localhost:5432/forgequeue_test?sslmode=disable",
		)
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, testDatabaseURL)
	if err != nil {
		log.Fatalf("failed to create test database pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		log.Fatalf("failed to connect to test database: %v", err)
	}

	var databaseName string

	err = pool.QueryRow(
		ctx,
		"SELECT current_database()",
	).Scan(&databaseName)
	if err != nil {
		pool.Close()
		log.Fatalf("failed to identify test database: %v", err)
	}

	if databaseName != "forgequeue_test" {
		pool.Close()
		log.Fatalf(
			"refusing to run destructive tests against database %q; expected %q",
			databaseName,
			"forgequeue_test",
		)
	}

	var jobsTableExists bool

	err = pool.QueryRow(
		ctx,
		"SELECT to_regclass('public.jobs') IS NOT NULL",
	).Scan(&jobsTableExists)
	if err != nil {
		pool.Close()
		log.Fatalf("failed to check test database schema: %v", err)
	}

	if !jobsTableExists {
		pool.Close()
		log.Fatal(
			"the jobs table does not exist in forgequeue_test; " +
				"apply migrations/001_create_jobs.sql before running tests",
		)
	}

	testPool = pool

	exitCode := m.Run()

	testPool.Close()
	os.Exit(exitCode)
}

func truncateJobs(t *testing.T) {
	t.Helper()

	_, err := testPool.Exec(
		context.Background(),
		"TRUNCATE TABLE jobs RESTART IDENTITY",
	)
	if err != nil {
		t.Fatalf("failed to truncate jobs table: %v", err)
	}
}

func createQueuedJob(t *testing.T, store *jobs.Store) jobs.Job {
	t.Helper()

	job, err := store.CreateJob(
		context.Background(),
		jobs.CreateJobRequest{
			Type:    "test.job",
			Payload: json.RawMessage(`{"foo":"bar"}`),
		},
	)
	if err != nil {
		t.Fatalf("failed to create test job: %v", err)
	}

	return job
}

func TestClaimNextQueuedJob_EmptyQueue(t *testing.T) {
	truncateJobs(t)

	store := jobs.NewStore(testPool)

	_, found, err := store.ClaimNextQueuedJob(
		context.Background(),
	)
	if err != nil {
		t.Fatalf("unexpected claim error: %v", err)
	}

	if found {
		t.Fatal("expected found=false when the queue is empty")
	}
}

func TestClaimNextQueuedJob_ClaimsQueuedJob(t *testing.T) {
	truncateJobs(t)

	store := jobs.NewStore(testPool)

	created := createQueuedJob(t, store)

	claimed, found, err := store.ClaimNextQueuedJob(context.Background())
	if err != nil {
		t.Fatalf("unexpected claim error: %v", err)
	}
	if !found {
		t.Fatal("expected found=true when a queued job exists")
	}

	// 1) What the claim call returned.
	if claimed.ID != created.ID {
		t.Errorf("claimed wrong job: got id=%d, want id=%d", claimed.ID, created.ID)
	}
	if claimed.Status != "running" {
		t.Errorf("claimed status: got %q, want %q", claimed.Status, "running")
	}

	// 2) What actually landed in the row.
	var (
		status       string
		attempts     int
		startedAtSet bool
		updatedAtSet bool
	)

	err = testPool.QueryRow(
		context.Background(),
		`SELECT status, attempts, started_at IS NOT NULL, updated_at IS NOT NULL
		 FROM jobs
		 WHERE id = $1`,
		created.ID,
	).Scan(&status, &attempts, &startedAtSet, &updatedAtSet)
	if err != nil {
		t.Fatalf("failed to read stored job: %v", err)
	}

	if status != "running" {
		t.Errorf("stored status: got %q, want %q", status, "running")
	}
	if attempts != 1 {
		t.Errorf("stored attempts: got %d, want 1", attempts)
	}
	if !startedAtSet {
		t.Error("expected started_at to be set")
	}
	if !updatedAtSet {
		t.Error("expected updated_at to be set")
	}
}

func TestMarkJobSucceeded(t *testing.T) {
	truncateJobs(t)

	store := jobs.NewStore(testPool)

	createQueuedJob(t, store)

	claimed, found, err := store.ClaimNextQueuedJob(context.Background())
	if err != nil {
		t.Fatalf("unexpected claim error: %v", err)
	}
	if !found {
		t.Fatal("expected to claim the queued job")
	}

	if err := store.MarkJobSucceeded(context.Background(), claimed.ID); err != nil {
		t.Fatalf("failed to mark job succeeded: %v", err)
	}

	var (
		status         string
		completedAtSet bool
	)

	err = testPool.QueryRow(
		context.Background(),
		`SELECT status, completed_at IS NOT NULL
		 FROM jobs
		 WHERE id = $1`,
		claimed.ID,
	).Scan(&status, &completedAtSet)
	if err != nil {
		t.Fatalf("failed to read stored job: %v", err)
	}

	if status != "succeeded" {
		t.Errorf("stored status: got %q, want %q", status, "succeeded")
	}
	if !completedAtSet {
		t.Error("expected completed_at to be set")
	}
}

// claimResult carries one goroutine's claim outcome back to the main test
// goroutine over a channel, so no assertions happen off the test goroutine.
type claimResult struct {
	job   jobs.WorkerJob
	found bool
	err   error
}

func TestClaimNextQueuedJob_ConcurrentClaimsUniqueJobs(t *testing.T) {
	truncateJobs(t)

	store := jobs.NewStore(testPool)

	const jobCount = 20

	for i := 0; i < jobCount; i++ {
		createQueuedJob(t, store)
	}

	results := make(chan claimResult, jobCount)

	var wg sync.WaitGroup
	for i := 0; i < jobCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, found, err := store.ClaimNextQueuedJob(context.Background())
			results <- claimResult{job: job, found: found, err: err}
		}()
	}

	wg.Wait()
	close(results)

	seen := make(map[int64]bool)
	claimed := 0
	for r := range results {
		if r.err != nil {
			t.Errorf("unexpected claim error: %v", r.err)
			continue
		}
		if !r.found {
			continue
		}
		claimed++
		if seen[r.job.ID] {
			t.Errorf("job id %d was claimed more than once", r.job.ID)
		}
		seen[r.job.ID] = true
	}

	if claimed != jobCount {
		t.Errorf("claimed %d jobs, want %d", claimed, jobCount)
	}
	if len(seen) != jobCount {
		t.Errorf("got %d unique claimed ids, want %d", len(seen), jobCount)
	}
}

func TestClaimNextQueuedJob_ConcurrentClaimsSingleJob(t *testing.T) {
	truncateJobs(t)

	store := jobs.NewStore(testPool)

	created := createQueuedJob(t, store)

	const workerCount = 20

	results := make(chan claimResult, workerCount)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, found, err := store.ClaimNextQueuedJob(context.Background())
			results <- claimResult{job: job, found: found, err: err}
		}()
	}

	wg.Wait()
	close(results)

	foundCount := 0
	notFoundCount := 0
	for r := range results {
		if r.err != nil {
			t.Errorf("unexpected claim error: %v", r.err)
			continue
		}
		if r.found {
			foundCount++
			if r.job.ID != created.ID {
				t.Errorf("claimed wrong job: got id=%d, want id=%d", r.job.ID, created.ID)
			}
		} else {
			notFoundCount++
		}
	}

	if foundCount != 1 {
		t.Errorf("expected exactly 1 successful claim, got %d", foundCount)
	}
	if notFoundCount != workerCount-1 {
		t.Errorf("expected %d found=false results, got %d", workerCount-1, notFoundCount)
	}
}
