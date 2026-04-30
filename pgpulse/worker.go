package pgpulse

import (
	"context"
	"log"
	"sync"
	"time"
)

// Handler processes a job. Return nil on success, or an error to trigger a retry.
type Handler func(ctx context.Context, job *Job) error

// WorkerConfig configures the worker pool.
type WorkerConfig struct {
	// Queue is the queue name to poll (default: "default").
	Queue string

	// Concurrency is the number of goroutines that process jobs (default: 5).
	Concurrency int

	// FetchSize is the number of jobs fetched per poll (default: same as Concurrency).
	FetchSize int

	// PollInterval is the time between polls when no jobs are found (default: 1s).
	PollInterval time.Duration

	// Logger for operational messages. If nil, the default log package is used.
	Logger *log.Logger
}

func (cfg *WorkerConfig) defaults() {
	if cfg.Queue == "" {
		cfg.Queue = "default"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	if cfg.FetchSize <= 0 {
		cfg.FetchSize = cfg.Concurrency
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
}

// Worker manages a pool of goroutines that poll and process jobs.
type Worker struct {
	client  *Client
	config  WorkerConfig
	handler Handler
}

// NewWorker creates a new Worker.
func NewWorker(client *Client, config WorkerConfig, handler Handler) *Worker {
	config.defaults()
	return &Worker{
		client:  client,
		config:  config,
		handler: handler,
	}
}

// Run starts the worker pool and blocks until ctx is cancelled.
// It gracefully waits for in-flight jobs to finish before returning.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	jobCh := make(chan *Job)

	// Start worker goroutines.
	for i := 0; i < w.config.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				w.process(ctx, job)
			}
		}()
	}

	// Poller loop.
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(jobCh)
			wg.Wait()
			return
		case <-ticker.C:
			jobs, err := w.client.Fetch(ctx, w.config.Queue, w.config.FetchSize)
			if err != nil {
				if ctx.Err() != nil {
					close(jobCh)
					wg.Wait()
					return
				}
				w.config.Logger.Printf("pgpulse: fetch error: %v", err)
				continue
			}
			for _, job := range jobs {
				select {
				case jobCh <- job:
				case <-ctx.Done():
					close(jobCh)
					wg.Wait()
					return
				}
			}
		}
	}
}

func (w *Worker) process(ctx context.Context, job *Job) {
	if err := w.handler(ctx, job); err != nil {
		w.config.Logger.Printf("pgpulse: job %d (kind=%s) failed: %v", job.ID, job.Kind, err)
		if fErr := w.client.Fail(ctx, job.ID, err); fErr != nil {
			w.config.Logger.Printf("pgpulse: could not fail job %d: %v", job.ID, fErr)
		}
		return
	}
	if err := w.client.Complete(ctx, job.ID); err != nil {
		w.config.Logger.Printf("pgpulse: could not complete job %d: %v", job.ID, err)
	}
}
