package pgpulse

import (
	"context"
	"testing"
	"time"
)

func TestWorkerConfigDefaults(t *testing.T) {
	cfg := WorkerConfig{}
	cfg.defaults()

	if cfg.Queue != "default" {
		t.Errorf("expected queue 'default', got %q", cfg.Queue)
	}
	if cfg.Concurrency != 5 {
		t.Errorf("expected concurrency 5, got %d", cfg.Concurrency)
	}
	if cfg.FetchSize != 5 {
		t.Errorf("expected fetch size 5, got %d", cfg.FetchSize)
	}
	if cfg.PollInterval != time.Second {
		t.Errorf("expected poll interval 1s, got %v", cfg.PollInterval)
	}
	if cfg.Logger == nil {
		t.Error("expected non-nil logger")
	}
}

func TestWorkerConfigCustomValues(t *testing.T) {
	cfg := WorkerConfig{
		Queue:        "emails",
		Concurrency:  10,
		FetchSize:    20,
		PollInterval: 500 * time.Millisecond,
	}
	cfg.defaults()

	if cfg.Queue != "emails" {
		t.Errorf("expected queue 'emails', got %q", cfg.Queue)
	}
	if cfg.Concurrency != 10 {
		t.Errorf("expected concurrency 10, got %d", cfg.Concurrency)
	}
	if cfg.FetchSize != 20 {
		t.Errorf("expected fetch size 20, got %d", cfg.FetchSize)
	}
}

func TestNewWorker(t *testing.T) {
	w := NewWorker(nil, WorkerConfig{}, func(ctx context.Context, job *Job) error {
		return nil
	})
	if w == nil {
		t.Fatal("expected non-nil worker")
	}
	if w.config.Concurrency != 5 {
		t.Errorf("expected default concurrency 5, got %d", w.config.Concurrency)
	}
}
