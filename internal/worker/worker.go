package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yukthi-Systems/Task-Alerts-Worker/internal/notify"
	"github.com/Yukthi-Systems/Task-Alerts-Worker/internal/store"
)

type Publisher interface {
	Publish(ctx context.Context, notificationType string, payload any) error
}

type Worker struct {
	Store      *store.Store
	Publisher  Publisher
	AppURL     string
	FCMAppName string // Firebase app the consumer sends push through
	BatchSize  int
	StaleAfter time.Duration
	SendAhead  time.Duration // claim alerts due up to this far ahead; must be ≥ the tick interval
	Parallel   int           // alerts published at the same time; below 1 means one at a time
}

// Tick processes due alerts until there are none left or a publish fails.
// An unsent alert stays unsent and is retried on the next tick, until it
// becomes stale.
func (w *Worker) Tick(ctx context.Context) {
	stale, err := w.Store.SkipStale(ctx, w.StaleAfter)
	if err != nil {
		slog.Error("skipping stale alerts failed", "err", err)
		return
	}
	if stale > 0 {
		slog.Warn("skipped stale alerts", "count", stale, "older_than", w.StaleAfter.String())
	}

	// Keep claiming full batches so a backlog clears in one tick.
	for {
		n, ok := w.batch(ctx)
		if !ok || n < w.BatchSize {
			return
		}
	}
}

// batch handles one claimed batch. It returns how many alerts were claimed and
// whether every one of them was sent.
func (w *Worker) batch(ctx context.Context) (int, bool) {
	start := time.Now()
	b, err := w.Store.ClaimDue(ctx, w.BatchSize, w.SendAhead)
	if err != nil {
		slog.Error("claiming due alerts failed", "err", err)
		return 0, false
	}
	defer b.Rollback(ctx)

	// Publish in parallel. The batch's transaction can't be used concurrently,
	// so alerts are marked sent afterwards, one by one.
	ok := w.publishAll(ctx, b.Alerts)

	sent := 0
	for i, a := range b.Alerts {
		if !ok[i] {
			continue
		}
		if err := b.MarkSent(ctx, a); err != nil {
			slog.Error("marking alert sent failed", "err", err)
			return len(b.Alerts), false
		}
		sent++
	}
	failed := len(b.Alerts) - sent

	if err := b.Commit(ctx); err != nil {
		slog.Error("committing batch failed; its alerts will be sent again", "sent", sent, "err", err)
		return len(b.Alerts), false
	}
	if len(b.Alerts) > 0 {
		slog.Info("batch done", "count", len(b.Alerts), "sent", sent, "failed", failed,
			"took_ms", time.Since(start).Milliseconds())
	}
	return len(b.Alerts), failed == 0
}

// publishAll sends the alerts, w.Parallel at a time, and reports which ones
// went out. After the first failure it starts no new alerts: RabbitMQ is most
// likely down, so instead of waiting on a timeout for every remaining alert,
// they're left unsent and retried next tick.
func (w *Worker) publishAll(ctx context.Context, alerts []store.Alert) []bool {
	ok := make([]bool, len(alerts))
	var failed atomic.Bool
	var wg sync.WaitGroup
	slots := make(chan struct{}, max(w.Parallel, 1))
	for i, a := range alerts {
		slots <- struct{}{}
		if failed.Load() {
			<-slots
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			if err := w.send(ctx, a); err != nil {
				failed.Store(true)
				slog.Error("publishing alert failed",
					"task_id", a.TaskID, "recipient_id", a.RecipientID, "alert_at", a.AlertAt, "err", err)
				return
			}
			ok[i] = true
		}()
	}
	wg.Wait()
	return ok
}

// send publishes every channel the alert asks for. If push fails after email
// went out, the whole alert is retried, so email can arrive twice; RabbitMQ
// failures are usually total, which keeps that rare.
func (w *Worker) send(ctx context.Context, a store.Alert) error {
	if a.ViaEmail {
		if err := w.Publisher.Publish(ctx, notify.TypeEmail, notify.EmailFor(a, w.AppURL, time.Now())); err != nil {
			return err
		}
	}
	if a.ViaPush {
		if len(a.FCMTokens) == 0 {
			slog.Info("no signed-in devices for push; skipping",
				"task_id", a.TaskID, "recipient_id", a.RecipientID)
			return nil
		}
		for _, p := range notify.PushesFor(a, w.AppURL, w.FCMAppName) {
			if err := w.Publisher.Publish(ctx, notify.TypePush, p); err != nil {
				return err
			}
		}
	}
	return nil
}
