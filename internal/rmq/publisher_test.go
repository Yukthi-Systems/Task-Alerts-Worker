package rmq

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublish(t *testing.T) {
	var gotPath, gotAuth string
	var got publishRequest
	routed := true

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &got); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		json.NewEncoder(w).Encode(map[string]bool{"routed": routed})
	}))
	defer srv.Close()

	p := NewPublisher(srv.URL, "user", "pass", "/", "notifications", "alerts")
	if err := p.Publish(context.Background(), "task_alert_email", map[string]int{"id": 1}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if gotPath != "/api/exchanges/%2F/notifications/publish" {
		t.Errorf("path = %q", gotPath)
	}
	if gotAuth != "Basic dXNlcjpwYXNz" {
		t.Errorf("auth = %q", gotAuth)
	}
	if got.Properties.Headers["type"] != "task_alert_email" || got.Properties.DeliveryMode != 2 {
		t.Errorf("properties = %+v", got.Properties)
	}
	if got.RoutingKey != "alerts" || got.PayloadEncoding != "string" || got.Payload != `{"id":1}` {
		t.Errorf("request = %+v", got)
	}

	routed = false
	err := p.Publish(context.Background(), "task_alert_email", nil)
	if err == nil || !strings.Contains(err.Error(), "not routed") {
		t.Errorf("unrouted publish: err = %v, want a not-routed error", err)
	}
}

func TestPublishHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"not_authorised"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()

	p := NewPublisher(srv.URL, "user", "bad", "/", "notifications", "alerts")
	err := p.Publish(context.Background(), "task_alert_push", nil)
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("err = %v, want a 401 error", err)
	}
}
