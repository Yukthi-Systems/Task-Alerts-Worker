package worker

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Yukthi-Systems/Task-Alerts-Worker/internal/notify"
	"github.com/Yukthi-Systems/Task-Alerts-Worker/internal/store"
)

type published struct {
	typ     string
	payload any
}

type fakePublisher struct {
	mu   sync.Mutex
	sent []published
	fail bool
}

func (f *fakePublisher) Publish(_ context.Context, typ string, payload any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return errors.New("rabbitmq down")
	}
	f.sent = append(f.sent, published{typ, payload})
	return nil
}

// TestTick runs against a real database loaded with Tasks-Main-API's
// docs/DB/schema.sql. It's skipped unless TEST_POSTGRES_URL is set, and it
// empties the tables it uses.
func TestTick(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec(`TRUNCATE users, tasks, task_alerts, sessions CASCADE`)

	const userID = "11111111-1111-1111-1111-111111111111"
	exec(`INSERT INTO users (user_id, email, domain, organization_id, organization_name, private_info, public_info)
	      VALUES ($1, 'raj@yukthi.com', 'yukthi.com', gen_random_uuid(), 'Yukthi', '{}', '{"name":"Rupraj"}')`, userID)
	exec(`INSERT INTO sessions (user_id, refresh_token, sso_token, fcm_token) VALUES
	      ($1, gen_random_uuid(), 's1', 'tok-live'),
	      ($1, gen_random_uuid(), 's2', NULL)`, userID)
	exec(`INSERT INTO sessions (user_id, refresh_token, sso_token, fcm_token, expires_at)
	      VALUES ($1, gen_random_uuid(), 's3', 'tok-expired', now() - interval '1 minute')`, userID)

	var parentID, taskID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO tasks (owner_id, created_by, title, details, task_tag, end_at)
		VALUES ($1, $1, 'Sprint 14', '{}', 'WORK', now()) RETURNING task_id`, userID).Scan(&parentID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO tasks (owner_id, created_by, parent_task_id, title, description, details, task_tag, start_at, end_at)
		VALUES ($1, $1, $2, 'Standup', 'Daily sync', '{}', 'WORK', '2026-10-29T10:30:00Z', '2026-10-29T11:00:00Z')
		RETURNING task_id`, userID, parentID).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	exec(`INSERT INTO tasks (owner_id, parent_task_id, title, details, task_tag, end_at)
	      SELECT $1, $2, 'Sub ' || g, '{}', 'WORK', now() FROM generate_series(1, 12) g`, userID, taskID)
	exec(`INSERT INTO task_assignees (task_id, assignee_id) VALUES ($1, $2)`, taskID, userID)
	exec(`INSERT INTO task_alerts (task_id, alert_recipient_id, alert_at, notify_via_email, notify_via_push) VALUES
	      ($1, $2, now() - interval '1 minute',  true,  true),  -- due
	      ($1, $2, now() + interval '4 minutes', false, true),  -- due before the next tick: sent early
	      ($1, $2, now() - interval '2 hours',   true,  true),  -- stale
	      ($1, $2, now() + interval '1 hour',    true,  true)   -- future`, taskID, userID)

	unsent := func() int {
		var n int
		pool.QueryRow(ctx, `SELECT count(*) FROM task_alerts WHERE notification_sent = false`).Scan(&n)
		return n
	}

	pub := &fakePublisher{fail: true}
	w := &Worker{Store: store.New(pool), Publisher: pub, AppURL: "https://app", FCMAppName: "calendar",
		BatchSize: 10, StaleAfter: time.Hour, SendAhead: 6 * time.Minute}

	// RabbitMQ down: the stale alert is dropped, the others stay unsent.
	w.Tick(ctx)
	if got := unsent(); got != 3 {
		t.Fatalf("after failed tick: %d unsent, want 3 (due + early + future)", got)
	}

	pub.fail = false
	w.Tick(ctx)
	if got := unsent(); got != 1 {
		t.Fatalf("after good tick: %d unsent, want 1 (future)", got)
	}
	if len(pub.sent) != 3 {
		t.Fatalf("published %d messages, want 3 (email + push, then the early push): %+v", len(pub.sent), pub.sent)
	}
	if pub.sent[0].typ != notify.TypeEmail || pub.sent[1].typ != notify.TypePush || pub.sent[2].typ != notify.TypePush {
		t.Errorf("types = %s, %s, %s; want email, fcm, fcm", pub.sent[0].typ, pub.sent[1].typ, pub.sent[2].typ)
	}
	email := pub.sent[0].payload.(notify.Email)
	v := email.Variables
	if email.To != "raj@yukthi.com" || v["name"] != "Rupraj" || v["task_title"] != "Standup" ||
		v["parent_task"] != "Sprint 14" || v["description"] != "Daily sync" || v["assignees"] != "Rupraj" ||
		v["owner"] != "Rupraj" || v["when"] != "Thu, 29 Oct 2026, 10:30 – 11:00 UTC" {
		t.Errorf("email = %+v", email)
	}
	if v["subtasks"] != "Sub 1, Sub 2, Sub 3, Sub 4, Sub 5, Sub 6, Sub 7, Sub 8, Sub 9, Sub 10 (+2 more)" {
		t.Errorf("subtasks = %q", v["subtasks"])
	}
	push := pub.sent[1].payload.(notify.Push)
	if push.Token != "tok-live" || push.AppName != "calendar" || push.Data["parent_task_id"] == "" {
		t.Errorf("push = %+v", push)
	}

	// Nothing more is due, so another tick publishes nothing.
	w.Tick(ctx)
	if len(pub.sent) != 3 {
		t.Errorf("re-tick published again: %d messages", len(pub.sent))
	}

	// A backlog published in parallel across several batches: each alert
	// goes out exactly once.
	exec(`INSERT INTO task_alerts (task_id, alert_recipient_id, alert_at, notify_via_email, notify_via_push)
	      SELECT $1, $2, now() - g * interval '1 second', true, false FROM generate_series(1, 45) g`, taskID, userID)
	w.Parallel = 8
	w.Tick(ctx)
	if got := unsent(); got != 1 {
		t.Fatalf("after parallel tick: %d unsent, want 1 (future)", got)
	}
	if len(pub.sent) != 3+45 {
		t.Errorf("parallel tick published %d messages, want 45", len(pub.sent)-3)
	}
}
