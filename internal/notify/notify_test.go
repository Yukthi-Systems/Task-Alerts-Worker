package notify

import (
	"testing"
	"time"

	"github.com/Yukthi-Systems/Task-Alerts-Worker/internal/store"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestWhen(t *testing.T) {
	tests := []struct {
		name, start, end, want string
	}{
		{"timed, same day", "2026-10-29T10:30:00Z", "2026-10-29T11:00:00Z", "Thu, 29 Oct 2026, 10:30 – 11:00 UTC"},
		{"timed, given in another zone", "2026-10-29T16:00:00+05:30", "2026-10-29T17:00:00+05:30", "Thu, 29 Oct 2026, 10:30 – 11:30 UTC"},
		{"timed, spans days", "2026-10-29T22:00:00Z", "2026-10-30T01:00:00Z", "Thu, 29 Oct 2026, 22:00 – Fri, 30 Oct 2026, 01:00 UTC"},
		{"all day, one day", "2026-10-29T00:00:00Z", "2026-10-29T00:00:00Z", "Thu, 29 Oct 2026 (all day)"},
		{"all day, several days", "2026-10-29T00:00:00Z", "2026-10-31T00:00:00Z", "Thu, 29 Oct 2026 – Sat, 31 Oct 2026 (all day)"},
		// The frontend nudges a timed instant at midnight UTC by one second.
		{"timed at nudged midnight", "2026-10-29T00:00:01Z", "2026-10-29T01:00:00Z", "Thu, 29 Oct 2026, 00:00 – 01:00 UTC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := When(mustTime(t, tt.start), mustTime(t, tt.end)); got != tt.want {
				t.Errorf("When() = %q, want %q", got, tt.want)
			}
		})
	}
}

func sampleAlert(t *testing.T) store.Alert {
	return store.Alert{
		TaskID:       123,
		TaskTitle:    "Write API docs",
		TaskStatus:   "IN_PROGRESS",
		TaskTag:      "WORK",
		TaskStart:    mustTime(t, "2026-10-29T10:30:00Z"),
		TaskEnd:      mustTime(t, "2026-10-29T11:00:00Z"),
		ParentTaskID: 100,
		ParentTitle:  "Sprint 14",
		OwnerName:    "Nikhil",
		Assignees:    []string{"Asha", "Rupraj"},
		UserEmail:    "raj@yukthi.com",
		UserName:     "Rupraj",
		FCMTokens:    []string{"tok1", "tok2"},
	}
}

func TestEmailFor(t *testing.T) {
	e := EmailFor(sampleAlert(t), "https://app.example.com", mustTime(t, "2026-10-29T10:25:00Z"))

	if e.To != "raj@yukthi.com" || e.Template != "task_alert" {
		t.Errorf("addressing wrong: %+v", e)
	}
	want := map[string]string{
		"name":        "Rupraj",
		"task_title":  "Write API docs",
		"parent_task": "Sprint 14",
		"when":        "Thu, 29 Oct 2026, 10:30 – 11:00 UTC",
		"status":      "In Progress",
		"tag":         "Work",
		"description": "—",
		"subtasks":    "—",
		"owner":       "Nikhil",
		"created_by":  "—",
		"assignees":   "Asha, Rupraj",
		"link":        "https://app.example.com/?task=123",
		"year":        "2026",
	}
	if len(e.Variables) != len(want) {
		t.Errorf("got %d variables, want %d: %v", len(e.Variables), len(want), e.Variables)
	}
	for k, v := range want {
		if e.Variables[k] != v {
			t.Errorf("variables[%q] = %q, want %q", k, e.Variables[k], v)
		}
	}
}

func TestPushesFor(t *testing.T) {
	pushes := PushesFor(sampleAlert(t), "https://app.example.com", "calendar")
	if len(pushes) != 2 {
		t.Fatalf("got %d pushes, want one per token", len(pushes))
	}
	p := pushes[1]
	if p.Token != "tok2" || p.AppName != "calendar" || p.Title != "Reminder: Write API docs" {
		t.Errorf("push = %+v", p)
	}
	if p.Body != "Subtask of Sprint 14 · Thu, 29 Oct 2026, 10:30 – 11:00 UTC" {
		t.Errorf("body = %q", p.Body)
	}
	if p.Data["notification_type"] != "TASK_ALERT" || p.Data["task_id"] != "123" || p.Data["parent_task_id"] != "100" {
		t.Errorf("data = %v", p.Data)
	}
}

func TestList(t *testing.T) {
	if got := list([]string{"a", "b"}, 5); got != "a, b (+3 more)" {
		t.Errorf("list = %q", got)
	}
	if got := label("ON_HOLD"); got != "On Hold" {
		t.Errorf("label = %q", got)
	}
}
