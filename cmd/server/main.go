package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"notification-webhook-service/internal/config"
	"notification-webhook-service/internal/database"
	"notification-webhook-service/internal/delivery"
	"notification-webhook-service/internal/handler"
	"notification-webhook-service/internal/observability"
	"notification-webhook-service/internal/queue"
	"notification-webhook-service/internal/worker"
)

const (
	httpShutdownTimeout   = 5 * time.Second
	consumerDrainTimeout  = 30 * time.Second
	workerShutdownTimeout = 3 * time.Second
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if err := run(); err != nil {
		slog.Error("service stopped with error", "error", observability.ErrorSummary(err))
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics, err := observability.NewMetrics()
	if err != nil {
		return fmt.Errorf("initialize metrics: %w", err)
	}

	dbPool, err := database.InitPool(ctx)
	if err != nil {
		return fmt.Errorf("initialize database pool: %w", err)
	}
	defer dbPool.Close()
	store := delivery.NewStore(dbPool)
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("apply database migrations: %w", err)
	}

	connector, err := queue.InitConnector(ctx)
	if err != nil {
		return fmt.Errorf("initialize required RabbitMQ connection: %w", err)
	}
	defer connector.Close()
	publisher, err := queue.NewPublisher(connector)
	if err != nil {
		return fmt.Errorf("initialize confirmed publisher: %w", err)
	}
	deadLetterPublisher, ok := publisher.(queue.DeadLetterPublisher)
	if !ok {
		return fmt.Errorf("publisher does not support dead-letter messages")
	}
	publisherHealth, ok := publisher.(queue.HealthReporter)
	if !ok {
		return fmt.Errorf("publisher does not report health")
	}
	consumer, err := worker.StartConsumer(ctx, connector, os.Getenv("WEBHOOK_SECRET"), store, deadLetterPublisher, metrics)
	if err != nil {
		return fmt.Errorf("start consumer: %w", err)
	}
	consumerDone := consumer.Done
	consumerHealthy := atomic.Bool{}
	consumerHealthy.Store(true)
	consumerFailed := observeWorker(ctx, consumerDone, func() { consumerHealthy.Store(false) }, consumer.Stopping)
	dispatcherCtx, cancelDispatcher := context.WithCancel(ctx)
	dispatcherDone := delivery.StartDispatcher(dispatcherCtx, store, publisher, metrics)
	backlogDone := delivery.StartOutboxBacklogSampler(dispatcherCtx, store, metrics, 15*time.Second)
	dispatcherHealthy := atomic.Bool{}
	dispatcherHealthy.Store(true)
	dispatcherFailed := observeWorker(dispatcherCtx, dispatcherDone, func() { dispatcherHealthy.Store(false) })

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery(), structuredRequestLog)
	router.GET("/health", handler.Health)
	router.GET("/ready", handler.Ready(dbPool, consumerHealthy.Load, dispatcherHealthy.Load, publisherHealth.Healthy))
	router.GET("/metrics", gin.WrapH(metrics.Handler()))
	router.POST("/event", handler.Event(store, metrics))
	srv := &http.Server{
		Addr:              config.GetPort(),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- srv.ListenAndServe() }()
	slog.Info("HTTP server started", "address", srv.Addr)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(quit)
	var stopReason error
	select {
	case <-consumerFailed:
		stopReason = errors.New("RabbitMQ consumer stopped unexpectedly")
		slog.Error("service worker stopped unexpectedly", "worker", "consumer")
	case <-dispatcherFailed:
		stopReason = errors.New("outbox dispatcher stopped unexpectedly")
		slog.Error("service worker stopped unexpectedly", "worker", "dispatcher")
	case sig := <-quit:
		slog.Info("shutdown signal received", "signal", sig.String())
	case err := <-serverDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stopReason = fmt.Errorf("HTTP server stopped unexpectedly: %w", err)
			slog.Error("HTTP server stopped unexpectedly", "error", observability.ErrorSummary(err))
		}
	}

	shutdownErr := shutdownServer(srv, consumer.StopAccepting, cancelDispatcher, cancel, consumerDone, dispatcherDone, backlogDone, shutdownTimeouts{
		http: httpShutdownTimeout, consumer: consumerDrainTimeout, worker: workerShutdownTimeout,
	})
	return errors.Join(stopReason, shutdownErr)
}

type shutdownTimeouts struct {
	http, consumer, worker time.Duration
}

func shutdownServer(server *http.Server, stopConsumer func(), cancelDispatcher, cancelService context.CancelFunc, consumerDone, dispatcherDone, backlogDone <-chan struct{}, timeouts shutdownTimeouts) error {
	var shutdownErrors []error
	httpCtx, stopHTTP := context.WithTimeout(context.Background(), timeouts.http)
	if err := server.Shutdown(httpCtx); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("drain HTTP server: %w", err))
		slog.Warn("HTTP graceful shutdown timed out; closing active connections", "error", observability.ErrorSummary(err))
		if closeErr := server.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("force-close HTTP server: %w", closeErr))
		}
	}
	stopHTTP()
	stopConsumer()
	cancelDispatcher()

	if err := waitForWorker(dispatcherDone, timeouts.worker); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("outbox dispatcher did not stop after cancel: %w", err))
	}
	if err := waitForWorker(backlogDone, timeouts.worker); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("outbox backlog sampler did not stop after cancel: %w", err))
	}
	if err := waitForWorker(consumerDone, timeouts.consumer); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("consumer drain timed out: %w", err))
	}
	cancelService()
	if err := waitForWorker(consumerDone, timeouts.worker); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("consumer did not stop after cancellation: %w", err))
	}
	if err := waitForWorker(dispatcherDone, timeouts.worker); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("outbox dispatcher did not stop after service cancellation: %w", err))
	}
	slog.Info("service shutdown complete")
	return errors.Join(shutdownErrors...)
}

func waitForWorker(done <-chan struct{}, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func structuredRequestLog(c *gin.Context) {
	started := time.Now()
	c.Next()
	slog.InfoContext(c.Request.Context(), "HTTP request completed",
		"method", c.Request.Method,
		"route", c.FullPath(),
		"status", c.Writer.Status(),
		"duration", time.Since(started),
	)
}

func observeWorker(ctx context.Context, done <-chan struct{}, onUnexpectedStop func(), intentionalStop ...<-chan struct{}) <-chan struct{} {
	failed := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-done:
			if ctx.Err() == nil {
				for _, stop := range intentionalStop {
					select {
					case <-stop:
						return
					default:
					}
				}
				onUnexpectedStop()
				close(failed)
			}
		}
	}()
	return failed
}
