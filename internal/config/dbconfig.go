package config

import "os"

// GetDatabaseURL returns the PostgreSQL connection URL.
// It reads the DATABASE_URL environment variable. If not set, an empty string is returned.
// The caller should decide how to handle the missing value (e.g., error out).
func GetDatabaseURL() string {
    return os.Getenv("DATABASE_URL")
}
