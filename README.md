# ForgeQueue

ForgeQueue is a PostgreSQL-backed distributed job queue written in Go: producers submit jobs
over an HTTP API, and concurrent worker goroutines claim them safely using
`SELECT ... FOR UPDATE SKIP LOCKED`.

**Status:** Week 1 complete — concurrent worker MVP with safe claiming and CI.

## Quick start

Requires Go 1.25+ and Docker.

**1. Start PostgreSQL**

```bash
docker compose up -d
```

**2. Apply the migration**

```bash
docker exec -i forgequeue-postgres \
  psql -U forgequeue -d forgequeue < migrations/001_create_jobs.sql
```

**3. Start the API** (listens on `:8080`)

```bash
go run ./cmd/api
```

**4. Start a worker** in a second terminal

```bash
go run ./cmd/worker --concurrency=5
```

`--concurrency` sets how many worker goroutines poll for jobs (default `1`). The worker runs
until interrupted; Ctrl+C stops it cleanly.

Both binaries read `DATABASE_URL`, defaulting to
`postgres://forgequeue:forgequeue@localhost:5432/forgequeue?sslmode=disable`.

## Submitting a job

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"test.success","payload":{"foo":"bar"}}'
```

```json
{"id":1,"status":"queued"}
```

`payload` is optional and defaults to `{}`. `type` is required. A worker picks the job up
within one polling interval (500ms) and logs the claim and completion.

There is also `GET /health`, which returns `ok`.

Inspect queue state directly until the inspection endpoints land in Week 4:

```bash
docker exec -it forgequeue-postgres \
  psql -U forgequeue -d forgequeue -c "SELECT status, count(*) FROM jobs GROUP BY status;"
```

## Job lifecycle

```
queued ──claim──> running ──> succeeded
                     └───────> failed
```

A job is created as `queued`. A worker claims it — atomically setting `running`, incrementing
`attempts`, and stamping `started_at` — runs it, then marks it `succeeded` or `failed`.

Retries, backoff, and `dead_lettered` arrive in Week 2; leases and crash recovery in Week 3.

## How safe claiming works

Every worker goroutine polls the same table, so the claim must guarantee that no two workers
get the same job. ForgeQueue does this with a single SQL statement that selects the oldest
`queued` row `FOR UPDATE SKIP LOCKED` and updates it to `running` in the same breath.

`SKIP LOCKED` is what makes it concurrent rather than merely correct: a worker that finds a
row already locked steps over it and takes the next free one instead of blocking. N workers
claim N distinct jobs with no coordination outside the database.

See [DECISIONS.md](DECISIONS.md) for the full reasoning, including why plain `FOR UPDATE`
would serialize the workers.

## Running the tests

```bash
go vet ./...
go test ./...
go test -race ./...
```

The `internal/jobs` tests run against a real database and **refuse to run** unless
`TEST_DATABASE_URL` points at a database named exactly `forgequeue_test` — they truncate
tables, so this guard exists to make it impossible to wipe a real one by accident.

Create it once:

```bash
docker exec -it forgequeue-postgres createdb -U forgequeue forgequeue_test
docker exec -i forgequeue-postgres \
  psql -U forgequeue -d forgequeue_test < migrations/001_create_jobs.sql
```

Then:

```bash
export TEST_DATABASE_URL="postgres://forgequeue:forgequeue@localhost:5432/forgequeue_test?sslmode=disable"
go test ./...
```

CI runs all three commands against a Postgres service container on every push and pull
request to `main`.

## Limitations

ForgeQueue provides **at-least-once** execution, not exactly-once. As of Week 1 a worker that
crashes mid-job leaves that job stuck in `running` with no recovery — leases and a reaper fix
this in Week 3. Job handlers should be written to be idempotent.

## Roadmap

- **Week 2** — retries with exponential backoff, dead-letter queue, idempotency keys
- **Week 3** — worker heartbeats, job leases, crash recovery
- **Week 4** — delayed jobs, inspection endpoints, `/metrics`
- **Week 5** — benchmarks, indexes, connection-pool analysis
- **Week 6** — final tests, docs, demos
