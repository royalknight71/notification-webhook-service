package main

import (
	"context"
	"log"
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
	"notification-webhook-service/internal/queue"
	"notification-webhook-service/internal/worker"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dbPool, err := database.InitPool(ctx)
	if err != nil {
		log.Fatalf("failed to initialize database pool: %v", err)
	}
	defer dbPool.Close()
	store := delivery.NewStore(dbPool)
	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("failed to apply database migrations: %v", err)
	}
	connector, err := queue.InitConnector(ctx)
	if err != nil {
		log.Fatalf("failed to initialize required RabbitMQ connection: %v", err)
	}
	defer connector.Close()
	publisher, err := queue.NewPublisher(connector)
	if err != nil {
		log.Fatalf("failed to initialize confirmed publisher: %v", err)
	}
	deadLetterPublisher, ok := publisher.(queue.DeadLetterPublisher)
	if !ok {
		log.Fatalf("publisher does not support dead-letter messages")
	}
	publisherHealth, ok := publisher.(queue.HealthReporter)
	if !ok {
		log.Fatalf("publisher does not report health")
	}
	consumerDone, err := worker.StartConsumer(ctx, connector, os.Getenv("WEBHOOK_SECRET"), store, deadLetterPublisher)
	if err != nil {
		log.Fatalf("failed to start consumer: %v", err)
	}
	consumerHealthy := atomic.Bool{}
	consumerHealthy.Store(true)
	consumerFailed := observeWorker(ctx, consumerDone, func() { consumerHealthy.Store(false) })
	dispatcherDone := delivery.StartDispatcher(ctx, store, publisher)
	dispatcherHealthy := atomic.Bool{}
	dispatcherHealthy.Store(true)
	dispatcherFailed := observeWorker(ctx, dispatcherDone, func() { dispatcherHealthy.Store(false) })

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.GET("/health", handler.Health)
	router.GET("/ready", handler.Ready(dbPool, consumerHealthy.Load, dispatcherHealthy.Load, publisherHealth.Healthy))
	router.POST("/event", handler.Event(store))
	srv := &http.Server{Addr: config.GetPort(), Handler: router}
	serverDone := make(chan error, 1)
	go func() { serverDone <- srv.ListenAndServe() }()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	select {
	case <-consumerFailed:
		log.Printf("RabbitMQ consumer stopped unexpectedly; shutting down")
	case <-dispatcherFailed:
		log.Printf("outbox dispatcher stopped unexpectedly; shutting down")
	case sig := <-quit:
		log.Printf("shutdown signal received: %v", sig)
	case err := <-serverDone:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed: %v", err)
		}
		return
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server forced to shutdown: %v", err)
	}
	cancel()
	select {
	case <-consumerDone:
	case <-shutdownCtx.Done():
		log.Printf("consumer did not stop before shutdown deadline")
	}
	select {
	case <-dispatcherDone:
	case <-shutdownCtx.Done():
		log.Printf("outbox dispatcher did not stop before shutdown deadline")
	}
}

func observeWorker(ctx context.Context, done <-chan struct{}, onUnexpectedStop func()) <-chan struct{} {
	failed := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-done:
			if ctx.Err() == nil {
				onUnexpectedStop()
				close(failed)
			}
		}
	}()
	return failed
}
