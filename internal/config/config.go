package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const MaxTick = 5 * time.Minute

type Config struct {
	PostgresURL string

	RMQDomain       string
	RMQUser         string
	RMQPassword     string
	RMQVirtualHost  string
	RMQExchangeName string
	RMQRoutingKey   string

	AppURL     string        // base of the link in the notification
	FCMAppName string        // Firebase app name in V3-Notifications-Worker's FIREBASE_APPS
	Tick       time.Duration // how often to poll for due alerts
	BatchSize  int           // max alerts claimed per tick
	Parallel   int           // alerts published at the same time
	StaleAfter time.Duration // alerts overdue by more than this are dropped
}

// Load reads the environment and reports every missing or invalid variable at once.
func Load() (Config, error) {
	var errs []string

	required := func(name string) string {
		v := strings.TrimSpace(os.Getenv(name))
		if v == "" {
			errs = append(errs, name+" must be set")
		}
		return v
	}

	optional := func(name, def string) string {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			return v
		}
		return def
	}

	positiveInt := func(name string, def int) int {
		v := strings.TrimSpace(os.Getenv(name))
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			errs = append(errs, name+" must be a positive integer")
		}
		return n
	}

	duration := func(name string, def time.Duration) time.Duration {
		v := strings.TrimSpace(os.Getenv(name))
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			errs = append(errs, name+" must be a positive duration such as 1h or 30m")
		}
		return d
	}

	cfg := Config{
		PostgresURL: required("POSTGRES_DB_URL"),

		RMQDomain:       required("RABBITMQ_DOMAIN"),
		RMQUser:         required("RABBITMQ_USER_NAME"),
		RMQPassword:     required("RABBITMQ_PASSWORD"),
		RMQVirtualHost:  required("RABBITMQ_VIRTUAL_HOST"),
		RMQExchangeName: required("RABBITMQ_EXCHANGE_NAME"),
		RMQRoutingKey:   required("RABBITMQ_ROUTING_KEY"),

		AppURL:     strings.TrimRight(required("APP_URL"), "/"),
		FCMAppName: optional("FCM_APP_NAME", "calendar"),
		Tick:       time.Duration(positiveInt("TICK_SECONDS", 300)) * time.Second,
		BatchSize:  positiveInt("BATCH_SIZE", 100),
		Parallel:   positiveInt("PUBLISH_PARALLEL", 10),
		StaleAfter: duration("STALE_AFTER", time.Hour),
	}

	if cfg.Tick > MaxTick {
		errs = append(errs, fmt.Sprintf("TICK_SECONDS must be at most %d, or new alerts can be sent late", int(MaxTick.Seconds())))
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("config: %s", strings.Join(errs, "; "))
	}
	return cfg, nil
}
