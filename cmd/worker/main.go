package main

import (
	"context"
	"log"

	"github.com/razvan4974/forgequeue/internal/db"
	"github.com/razvan4974/forgequeue/internal/jobs"
	"github.com/razvan4974/forgequeue/internal/worker"
)

func main() {
	ctx := context.Background()

	dbpool, err := db.NewPostgresPool(ctx)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer dbpool.Close()

	jobStore := jobs.NewStore(dbpool)
	w := worker.New(jobStore)

	if err := w.RunOnce(ctx); err != nil {
		log.Fatalf("worker failed: %v", err)
	}
}
