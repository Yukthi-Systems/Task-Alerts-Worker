package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Yukthi-Systems/Task-Alerts-Worker/internal/config"
	"github.com/Yukthi-Systems/Task-Alerts-Worker/internal/rmq"
	"github.com/Yukthi-Systems/Task-Alerts-Worker/internal/store"
	"github.com/Yukthi-Systems/Task-Alerts-Worker/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PostgresURL)
	if err != nil {
		slog.Error("connecting to postgres failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	w := &worker.Worker{
		Store: store.New(pool),
		Publisher: rmq.NewPublisher("https://"+cfg.RMQDomain, cfg.RMQUser, cfg.RMQPassword,
			cfg.RMQVirtualHost, cfg.RMQExchangeName, cfg.RMQRoutingKey),
		AppURL:     cfg.AppURL,
		FCMAppName: cfg.FCMAppName,
		BatchSize:  cfg.BatchSize,
		StaleAfter: cfg.StaleAfter,
		SendAhead:  cfg.Tick + time.Minute,
	}

	slog.Info("alerts worker started",
		"tick", cfg.Tick.String(), "send_ahead", w.SendAhead.String(),
		"batch_size", cfg.BatchSize, "stale_after", cfg.StaleAfter.String())

	ticker := time.NewTicker(cfg.Tick)
	defer ticker.Stop()
	for {
		// A tick isn't cancelled by SIGTERM, so the current batch finishes
		// before the worker exits.
		w.Tick(context.WithoutCancel(ctx))

		select {
		case <-ctx.Done():
			slog.Info("alerts worker stopped")
			return
		case <-ticker.C:
		}
	}
}
