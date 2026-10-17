package main

import (
    "github.com/gin-gonic/gin"
    "notification-webhook-service/internal/config"
    "notification-webhook-service/internal/handler"
)

func main() {
    // Initialize Gin router
    router := gin.Default()

    // Register health endpoint
    router.GET("/health", handler.Health)

    // Run the server on configured port
    if err := router.Run(config.GetPort()); err != nil {
        panic(err)
    }
}