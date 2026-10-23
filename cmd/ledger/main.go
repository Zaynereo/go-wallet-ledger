// Command ledger serves the wallet API and runs operational subcommands.
//
//	ledger serve       HTTP API and outbox relay (default)
//	ledger seed        create the demo accounts
//	ledger reconcile   print the invariant report; exit 2 when it is not ok
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Zaynereo/go-wallet-ledger/internal/config"
	"github.com/Zaynereo/go-wallet-ledger/internal/httpapi"
	"github.com/Zaynereo/go-wallet-ledger/internal/ledger"
	"github.com/Zaynereo/go-wallet-ledger/internal/migrate"
	"github.com/Zaynereo/go-wallet-ledger/internal/outbox"
	"github.com/Zaynereo/go-wallet-ledger/internal/seed"
	"github.com/Zaynereo/go-wallet-ledger/internal/store/mysqlstore"
)

var errInvariants = errors.New("reconciliation found broken invariants")

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, errInvariants) {
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)}))
	slog.SetDefault(logger)

	cmd := "serve"
	if len(args) > 0 {
		cmd = args[0]
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "serve":
		return serve(ctx, cfg, logger)
	case "seed":
		return seedCommand(ctx, cfg)
	case "reconcile":
		return reconcileCommand(ctx, cfg)
	default:
		return fmt.Errorf("unknown command %q (want serve, seed, or reconcile)", cmd)
	}
}

func serve(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	store, svc, err := openApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	publisher, err := newPublisher(cfg, logger)
	if err != nil {
		return err
	}
	defer func() { _ = publisher.Close() }()

	relay := outbox.NewRelay(store, publisher, cfg.OutboxBatch, cfg.OutboxInterval, cfg.OutboxLease, logger)
	relayCtx, relayCancel := context.WithCancel(context.Background())
	defer relayCancel()
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		relay.Run(relayCtx)
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.New(svc, store.Ping, logger).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.HTTPAddr, "publisher", cfg.Publisher)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-errCh:
		relayCancel()
		<-relayDone
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Error("http shutdown", "err", err)
	}
	relayCancel()
	<-relayDone
	if err := relay.Drain(shutCtx); err != nil {
		logger.Error("outbox drain", "err", err)
	}
	return nil
}

func seedCommand(ctx context.Context, cfg config.Config) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store, svc, err := openApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	accounts, err := seed.Run(ctx, svc)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(accounts)
}

func reconcileCommand(ctx context.Context, cfg config.Config) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store, svc, err := openApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	report, err := svc.Reconcile(ctx)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		return err
	}
	if !report.OK {
		return errInvariants
	}
	return nil
}

func openApp(ctx context.Context, cfg config.Config, logger *slog.Logger) (*mysqlstore.Store, *ledger.Service, error) {
	store, err := mysqlstore.Open(cfg.MySQLDSN, mysqlstore.Options{
		LockWaitTimeout: cfg.LockWaitTimeout,
		Logger:          logger,
		MaxOpenConns:    cfg.DBMaxOpenConns,
		MaxIdleConns:    cfg.DBMaxIdleConns,
	})
	if err != nil {
		return nil, nil, err
	}
	mctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := migrate.Up(mctx, store.DB()); err != nil {
		_ = store.Close()
		return nil, nil, err
	}
	return store, ledger.New(store), nil
}

func newPublisher(cfg config.Config, logger *slog.Logger) (outbox.Publisher, error) {
	if cfg.Publisher == "kafka" {
		return outbox.NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic), nil
	}
	return outbox.NewLogPublisher(logger), nil
}

func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
