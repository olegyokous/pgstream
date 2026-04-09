package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/user/pgstream/internal/wal"
)

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pipeline, err := wal.NewPipeline(wal.PipelineConfig{
		Filter: wal.FilterConfig{
			Tables:  splitCSV(cfg.tables),
			Actions: splitCSV(cfg.actions),
		},
		Format: cfg.format,
		Writer: os.Stdout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgstream: %v\n", err)
		os.Exit(1)
	}

	listener := wal.NewListener(wal.ListenerConfig{
		DSN:             cfg.dsn,
		SlotName:        cfg.slot,
		PublicationName: cfg.publication,
	})

	if err := listener.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "pgstream: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close(context.Background())

	msgCh := make(chan wal.Message, 64)

	errCh := make(chan error, 2)
	go func() { errCh <- listener.Start(ctx, msgCh) }()
	go func() { errCh <- pipeline.Run(ctx, msgCh) }()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil && err != context.Canceled {
			fmt.Fprintf(os.Stderr, "pgstream: %v\n", err)
		}
	}
}

type config struct {
	dsn         string
	slot        string
	publication string
	format      string
	tables      string
	actions     string
}

func loadConfig() config {
	getenv := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	return config{
		dsn:         getenv("PGSTREAM_DSN", "postgres://localhost/postgres"),
		slot:        getenv("PGSTREAM_SLOT", "pgstream"),
		publication: getenv("PGSTREAM_PUBLICATION", "pgstream_pub"),
		format:      getenv("PGSTREAM_FORMAT", "json"),
		tables:      getenv("PGSTREAM_TABLES", ""),
		actions:     getenv("PGSTREAM_ACTIONS", ""),
	}
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			result = append(result, t)
		}
	}
	return result
}
