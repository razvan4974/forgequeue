package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/razvan4974/forgequeue/internal/db"
	"github.com/razvan4974/forgequeue/internal/jobs"
)

func main() {
	ctx := context.Background()

	dbpool, err := db.NewPostgresPool(ctx)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer dbpool.Close()

	jobStore := jobs.NewStore(dbpool)

	r := chi.NewRouter()

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	r.Post("/jobs", func(w http.ResponseWriter, r *http.Request) {
		createJobHandler(w, r, jobStore)
	})

	log.Println("ForgeQueue API Running on :8080")

	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func createJobHandler(w http.ResponseWriter, r *http.Request, store *jobs.Store) {
	var req jobs.CreateJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.Type == "" {
		http.Error(w, "job type is required", http.StatusBadRequest)
		return
	}

	job, err := store.CreateJob(r.Context(), req)
	if err != nil {
		log.Printf("failed to create job: %v", err)
		http.Error(w, "failed to create the job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(job); err != nil {
		log.Printf("failed to encode response %v", err)
	}
}
