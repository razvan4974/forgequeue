# ForgeQueue

ForgeQueue is a reliable background job processing system built with Go and PostgreSQL.

## Day 1 Scope

The first version allows clients to create jobs through an API and store them in PostgreSQL.

## Current goal

- POST /jobs accepts a job
- The job is stored with status `queued`
- The API returns the job ID and status

## Future work

- Worker process
- Safe concurrent job claiming
- Retries
- Dead-letter queue
- Heartbeats
- Metrics
- Benchmarks