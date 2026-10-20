package main

import (
    "context"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/gin-gonic/gin"
    "notification-webhook-service/internal/config"
    "notification-webhook-service/internal/handler"
    "notification-webhook-service/internal/database"
    "notification-webhook-service/internal/queue"
    "notification-webhook-service/internal/worker"
)

func main() {
    // Create a base context that we can cancel on shutdown.
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Initialize Gin router.
    router := gin.Default()

    // Register health and readiness endpoints.
    router.GET("/health", handler.Health)


    // Initialize database connection pool.
    dbPool, err := database.InitPool(ctx)
    if err != nil {
        log.Fatalf("failed to initialize database pool: %v", err)
    }
    defer dbPool.Close()

    // Re-register readiness now that we have the pool.
    router.GET("/ready", handler.Ready(dbPool))

    // RabbitMQ connector – non‑fatal.
    var publisher queue.Publisher
    var connector *queue.Connector
    if c, err := queue.InitConnector(ctx); err != nil {
        log.Printf("warning: RabbitMQ not available – %v (publishing disabled)", err)
    } else {
        connector = c
        defer connector.Close()
        publisher = queue.NewPublisher(connector)
        // Start background consumer.
        webhookSecret := os.Getenv("WEBHOOK_SECRET")
        if err := worker.StartConsumer(ctx, connector, webhookSecret); err != nil {
            log.Fatalf("failed to start consumer: %v", err)
        }
    }

    // Register event endpoint – works even if publisher is nil.
    router.POST("/event", handler.Event(publisher))

    // Set up HTTP server for graceful shutdown.
    srv := &http.Server{Addr: config.GetPort(), Handler: router}

    // Run server in a separate goroutine.
    go func() {
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("failed to start server: %v", err)
        }
    }()

    // Wait for interrupt signal.
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
    <-quit
    log.Println("shutdown signal received, exiting...")
    // Gracefully shut down server with timeout.
    shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer shutdownCancel()
    if err := srv.Shutdown(shutdownCtx); err != nil {
        log.Printf("server forced to shutdown: %v", err)
    }
    // cancel() will signal consumers to stop via the shared ctx.
    cancel()
}