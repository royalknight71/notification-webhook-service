package main

import (
    "context"
    "log"

    "github.com/gin-gonic/gin"
    "notification-webhook-service/internal/config"
    "notification-webhook-service/internal/handler"
    "notification-webhook-service/internal/database"
)

func main() {
    // Initialize Gin router
    router := gin.Default()

    // Register health endpoint
    router.GET("/health", handler.Health)

    // Initialize database connection pool
    ctx := context.Background()
    dbPool, err := database.InitPool(ctx)
    if err != nil {
        log.Fatalf("failed to initialize database pool: %v", err)
    }
    defer dbPool.Close()

    // Register readiness endpoint (uses the pool for DB ping)
    router.GET("/ready", handler.Ready(dbPool))

    // Run the server on the configured port
    if err := router.Run(config.GetPort()); err != nil {
        log.Fatalf("failed to start server: %v", err)
    }
}