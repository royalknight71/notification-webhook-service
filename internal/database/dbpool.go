package database

import (
    "context"
    "fmt"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
    "notification-webhook-service/internal/config"
)

// InitPool creates a pgx connection pool using DATABASE_URL.
// It validates the URL, configures a modest pool size, and pings the DB
// to ensure connectivity before returning the pool.
func InitPool(ctx context.Context) (*pgxpool.Pool, error) {
    dbURL := config.GetDatabaseURL()
    if dbURL == "" {
        return nil, fmt.Errorf("DATABASE_URL not set")
    }

    cfg, err := pgxpool.ParseConfig(dbURL)
    if err != nil {
        return nil, fmt.Errorf("invalid DATABASE_URL: %w", err)
    }
    // Simple pool tuning – these values are reasonable defaults for a dev env.
    cfg.MaxConns = 10
    cfg.MinConns = 2
    cfg.MaxConnLifetime = 30 * time.Minute

    pool, err := pgxpool.NewWithConfig(ctx, cfg)
    if err != nil {
        return nil, fmt.Errorf("failed to create pgx pool: %w", err)
    }

    // Verify the connection with a short timeout.
    pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()
    if err := pool.Ping(pingCtx); err != nil {
        pool.Close()
        return nil, fmt.Errorf("cannot ping database: %w", err)
    }
    return pool, nil
}
