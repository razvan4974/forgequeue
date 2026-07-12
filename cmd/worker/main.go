package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/razvan4974/forgequeue/internal/db"
	"github.com/razvan4974/forgequeue/internal/jobs"
	"github.com/razvan4974/forgequeue/internal/worker"
)

func main() {
	concurrency := flag.Int("concurrency", 1, "number of worker goroutines")
	flag.Parse()

	if *concurrency < 1 {
		log.Fatalf("concurrency must be at least one")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbpool, err := db.NewPostgresPool(ctx)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer dbpool.Close()

	jobStore := jobs.NewStore(dbpool)
	w := worker.New(jobStore)

	log.Printf("worker starting with concurrency=%d", *concurrency)

	w.Run(ctx, *concurrency)

	log.Println("worker stopped")

}
