# Decisions

A running log of design decisions, why they were made, and what they cost.

---

## 1. Claiming jobs with `FOR UPDATE SKIP LOCKED`

**Date:** Week 1
**Status:** Accepted
**Code:** `internal/jobs/store.go` — `ClaimNextQueuedJob`

### The problem

ForgeQueue runs N worker goroutines against a single `jobs` table. The naive claim is
"read the oldest `queued` row, then update it to `running`". With more than one worker
this breaks immediately: every worker reads the *same* oldest row, and every worker
believes it owns that job. One job gets executed N times.

### Why not plain `FOR UPDATE`

`SELECT ... FOR UPDATE` fixes correctness — only one transaction can hold the row lock, so
only one worker wins. But the losers do not move on. They **block** waiting for the lock
holder to commit, then re-read the row, find it is no longer `queued`, and start over.

The result is that all N workers queue up behind the same row. Effective throughput
collapses towards single-worker throughput no matter how high `--concurrency` is set. The
concurrency is real in Go and imaginary in the database.

### Why `SKIP LOCKED`

`SELECT ... FOR UPDATE SKIP LOCKED` tells PostgreSQL: if a candidate row is already locked
by another transaction, do not wait for it — skip it and consider the next row.

So with 20 queued jobs and 20 workers claiming simultaneously, worker 1 locks job 1,
worker 2 skips job 1 and locks job 2, and so on. Twenty workers claim twenty distinct jobs
with zero blocking and zero coordination between them.

This is the entire reason ForgeQueue can use PostgreSQL as the queue rather than bolting on
Redis, ZooKeeper, or an application-level lock service. The database *is* the coordinator.

Verified by two tests in `internal/jobs/store_test.go`:

- `TestClaimNextQueuedJob_ConcurrentClaimsUniqueJobs` — 20 workers, 20 jobs, 20 unique IDs
- `TestClaimNextQueuedJob_ConcurrentClaimsSingleJob` — 20 workers, 1 job, exactly 1 winner

### Why the claim is a single statement

The claim is written as a CTE that selects the row and then updates it in the same
statement:

```sql
WITH next_job AS (
    SELECT id FROM jobs
    WHERE status = 'queued'
    ORDER BY created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs
SET status = 'running', attempts = attempts + 1, started_at = NOW(), updated_at = NOW()
FROM next_job
WHERE jobs.id = next_job.id
RETURNING ...
```

Selecting and marking in one statement means there is never a moment where a row is locked
but still says `queued`. The lock and the status change land together, and the statement is
implicitly its own transaction. A separate `SELECT` then `UPDATE` would need an explicit
transaction to get the same guarantee, and would hold the lock across a network round trip
for no benefit.

`ORDER BY created_at ASC` gives rough FIFO ordering. It is rough rather than exact by
design: `SKIP LOCKED` means a worker may take job 3 while job 2 is briefly locked. Strict
global ordering is incompatible with parallel claiming, and ForgeQueue chooses parallelism.

### What this costs

**At-least-once, not exactly-once.** A worker that crashes after claiming a job leaves the
row stuck in `running` forever. Nothing in Week 1 detects or recovers that — the job is
simply lost until someone fixes it by hand.

This is a known and accepted Week 1 limitation, not an oversight. Week 3 addresses it with
job leases (`locked_by`, `locked_until`) and a reaper that requeues rows whose lease has
expired. Even then the guarantee is at-least-once: a job may run twice if a worker stalls
long enough for its lease to expire while it is still working. Handlers must be idempotent.

---

## 2. Polling instead of `LISTEN`/`NOTIFY`

**Date:** Week 1
**Status:** Accepted
**Code:** `internal/worker/worker.go` — `pollInterval`

Workers poll every 500ms rather than being woken by PostgreSQL's `LISTEN`/`NOTIFY`.

Polling is chosen deliberately for the MVP. It has no extra moving parts, no dedicated
listener connection per worker, no reconnect logic when the notification channel drops, and
it degrades gracefully — a missed notification is invisible because the next poll picks the
job up anyway. `LISTEN`/`NOTIFY` needs all of that handled correctly *and* still needs a
polling fallback for exactly that reason.

The cost is latency: a job submitted just after a poll waits up to 500ms before any worker
sees it, and idle workers generate constant no-op queries against the database.

`LISTEN`/`NOTIFY` is tracked as an explicit optional item after the six-week core. The plan
there is to keep the poll loop as a fallback and measure enqueue-to-start latency before and
after, so the improvement is a number rather than an assumption.

---

## Week 1 verification run

**Date:** 2026-07-19
**Environment:** Linux x86_64, 8 cores, Go 1.26.0, PostgreSQL 16 (docker compose)

100 jobs submitted via `POST /jobs`, drained by a single worker process started with
`--concurrency=5`. Each job sleeps 1s in `processJob`.

| Metric | Expected | Observed |
| --- | --- | --- |
| Jobs submitted | 100 | 100 |
| Jobs `succeeded` | 100 | 100 |
| Jobs stuck in `running` | 0 | 0 |
| Jobs left `queued` | 0 | 0 |
| Distinct job IDs in claim log | 100 | 100 |
| **Duplicate claims** | **0** | **0** |
| Rows with `attempts <> 1` | 0 | 0 |
| Drain time | — | 20.4s |
| Throughput | — | 4.91 jobs/sec |

Throughput of 4.91 jobs/sec against 5 goroutines each sleeping 1s per job is essentially the
theoretical ceiling (5.0), so claiming contention is not a measurable bottleneck at this
scale. Nothing here is a real performance result — the work is a `time.Sleep`. Week 5 covers
actual benchmarks.

Duplicate claims were checked two ways: `sort | uniq -d` over the `claimed job id=` lines in
the worker log, and a `SELECT count(*) FROM jobs WHERE attempts <> 1` on the database. Both
came back clean, which is the Week 1 exit criterion.

The worker was stopped with `SIGTERM` mid-idle and printed `worker stopped` before exiting,
confirming graceful shutdown.

**One issue found:** the run produced 250 `no queueed jobs found` lines during roughly 24
seconds of idle time — the log fires on every poll of every goroutine rather than once per
idle period. Tracked as a follow-up in `internal/worker/worker.go`; the fix is to log only
on the transition into idle, not on every empty poll.
