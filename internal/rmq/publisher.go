package rmq

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Publisher struct {
	baseURL     string // e.g. https://rmq.example.com; tests point this at httptest
	virtualHost string
	exchange    string
	routingKey  string
	auth        string
	client      *http.Client
}

func NewPublisher(baseURL, user, password, virtualHost, exchange, routingKey string) *Publisher {
	return &Publisher{
		baseURL:     baseURL,
		virtualHost: virtualHost,
		exchange:    exchange,
		routingKey:  routingKey,
		auth:        "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password)),
		client:      &http.Client{Timeout: 10 * time.Second},
	}
}

type publishRequest struct {
	VHost           string     `json:"vhost"`
	Name            string     `json:"name"`
	Properties      properties `json:"properties"`
	RoutingKey      string     `json:"routing_key"`
	Payload         string     `json:"payload"`
	PayloadEncoding string     `json:"payload_encoding"`
}

type properties struct {
	DeliveryMode int               `json:"delivery_mode"`
	Headers      map[string]string `json:"headers"`
}

// Publish sends payload (encoded as a JSON string) with the given "type" header.
// It succeeds only if RabbitMQ answers 2xx and reports the message as routed to a queue.
func (p *Publisher) Publish(ctx context.Context, notificationType string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}
	body, err := json.Marshal(publishRequest{
		VHost: p.virtualHost,
		Name:  p.exchange,
		Properties: properties{
			DeliveryMode: 2,
			Headers:      map[string]string{"type": notificationType},
		},
		RoutingKey:      p.routingKey,
		Payload:         string(encoded),
		PayloadEncoding: "string",
	})
	if err != nil {
		return fmt.Errorf("encode publish request: %w", err)
	}

	endpoint := fmt.Sprintf("%s/api/exchanges/%s/%s/publish",
		p.baseURL, url.PathEscape(p.virtualHost), url.PathEscape(p.exchange))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", p.auth)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("publish %s: %w", notificationType, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("publish %s: rabbitmq returned %d: %s", notificationType, resp.StatusCode, respBody)
	}
	var result struct {
		Routed bool `json:"routed"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("publish %s: unreadable response %q: %w", notificationType, respBody, err)
	}
	if !result.Routed {
		return fmt.Errorf("publish %s: message was not routed to any queue (check exchange %q and routing key %q)",
			notificationType, p.exchange, p.routingKey)
	}
	return nil
}
