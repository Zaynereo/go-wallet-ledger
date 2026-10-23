package config

import (
	"testing"
)

func TestLoadRequiresDSN(t *testing.T) {
	t.Setenv("MYSQL_DSN", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected missing dsn error")
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv("MYSQL_DSN", "user:password@tcp(127.0.0.1:3306)/ledger?parseTime=true&charset=utf8mb4&loc=UTC")
	t.Setenv("PUBLISHER", "")
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("LOG_LEVEL", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Publisher != "log" || cfg.HTTPAddr != ":8080" || cfg.LogLevel != "info" {
		t.Fatalf("%+v", cfg)
	}
	if cfg.OutboxBatch != 50 || cfg.KafkaTopic != "ledger.events" {
		t.Fatalf("%+v", cfg)
	}
}

func TestLoadKafkaRequiresBrokers(t *testing.T) {
	t.Setenv("MYSQL_DSN", "user:password@tcp(127.0.0.1:3306)/ledger?parseTime=true&loc=UTC")
	t.Setenv("PUBLISHER", "kafka")
	t.Setenv("KAFKA_BROKERS", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected broker error")
	}
	t.Setenv("KAFKA_BROKERS", " localhost:19092, redpanda:9092 ")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "localhost:19092" {
		t.Fatalf("%v", cfg.KafkaBrokers)
	}
}

func TestLoadRejectsBadDSN(t *testing.T) {
	t.Setenv("MYSQL_DSN", "user:password@tcp(127.0.0.1:3306)/ledger")
	if _, err := Load(); err == nil {
		t.Fatal("expected parseTime error")
	}
}
