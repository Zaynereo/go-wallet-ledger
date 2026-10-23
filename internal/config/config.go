// Package config loads process settings from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the process configuration. Durations use Go's time.ParseDuration
// syntax (for example 500ms, 10s).
type Config struct {
	HTTPAddr        string
	MySQLDSN        string
	Publisher       string
	KafkaBrokers    []string
	KafkaTopic      string
	LogLevel        string
	OutboxInterval  time.Duration
	OutboxBatch     int
	OutboxLease     time.Duration
	ShutdownTimeout time.Duration
	LockWaitTimeout time.Duration
	DBMaxOpenConns  int
	DBMaxIdleConns  int
}

// Load reads environment variables and checks required combinations.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:        getenv("HTTP_ADDR", ":8080"),
		MySQLDSN:        strings.TrimSpace(os.Getenv("MYSQL_DSN")),
		Publisher:       strings.ToLower(getenv("PUBLISHER", "log")),
		KafkaBrokers:    splitCSV(os.Getenv("KAFKA_BROKERS")),
		KafkaTopic:      getenv("KAFKA_TOPIC", "ledger.events"),
		LogLevel:        strings.ToLower(getenv("LOG_LEVEL", "info")),
		OutboxInterval:  500 * time.Millisecond,
		OutboxBatch:     50,
		OutboxLease:     30 * time.Second,
		ShutdownTimeout: 15 * time.Second,
		LockWaitTimeout: 10 * time.Second,
		DBMaxOpenConns:  25,
		DBMaxIdleConns:  10,
	}

	var err error
	if cfg.OutboxInterval, err = durationEnv("OUTBOX_POLL_INTERVAL", cfg.OutboxInterval); err != nil {
		return Config{}, err
	}
	if cfg.OutboxLease, err = durationEnv("OUTBOX_LEASE", cfg.OutboxLease); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = durationEnv("SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.LockWaitTimeout, err = durationEnv("LOCK_WAIT_TIMEOUT", cfg.LockWaitTimeout); err != nil {
		return Config{}, err
	}
	if cfg.OutboxBatch, err = intEnv("OUTBOX_BATCH_SIZE", cfg.OutboxBatch); err != nil {
		return Config{}, err
	}
	if cfg.DBMaxOpenConns, err = intEnv("DB_MAX_OPEN_CONNS", cfg.DBMaxOpenConns); err != nil {
		return Config{}, err
	}
	if cfg.DBMaxIdleConns, err = intEnv("DB_MAX_IDLE_CONNS", cfg.DBMaxIdleConns); err != nil {
		return Config{}, err
	}

	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return Config{}, fmt.Errorf("LOG_LEVEL must be debug, info, warn, or error")
	}
	if cfg.MySQLDSN == "" {
		return Config{}, fmt.Errorf("MYSQL_DSN is required")
	}
	if !strings.Contains(cfg.MySQLDSN, "parseTime=true") {
		return Config{}, fmt.Errorf("MYSQL_DSN must include parseTime=true")
	}
	if !strings.Contains(cfg.MySQLDSN, "loc=UTC") {
		return Config{}, fmt.Errorf("MYSQL_DSN must include loc=UTC")
	}
	switch cfg.Publisher {
	case "log":
	case "kafka":
		if len(cfg.KafkaBrokers) == 0 {
			return Config{}, fmt.Errorf("KAFKA_BROKERS is required when PUBLISHER=kafka")
		}
		if cfg.KafkaTopic == "" {
			return Config{}, fmt.Errorf("KAFKA_TOPIC is required when PUBLISHER=kafka")
		}
	default:
		return Config{}, fmt.Errorf("PUBLISHER must be log or kafka")
	}
	if cfg.OutboxBatch < 1 || cfg.OutboxBatch > 1000 {
		return Config{}, fmt.Errorf("OUTBOX_BATCH_SIZE must be between 1 and 1000")
	}
	if cfg.OutboxInterval <= 0 || cfg.OutboxLease <= 0 || cfg.ShutdownTimeout <= 0 || cfg.LockWaitTimeout < time.Second {
		return Config{}, fmt.Errorf("durations must be positive and LOCK_WAIT_TIMEOUT must be at least 1s")
	}
	if cfg.DBMaxOpenConns < 1 || cfg.DBMaxIdleConns < 0 || cfg.DBMaxIdleConns > cfg.DBMaxOpenConns {
		return Config{}, fmt.Errorf("invalid database pool settings")
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func intEnv(key string, fallback int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func splitCSV(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
