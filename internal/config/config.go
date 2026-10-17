package config

import (
    "os"
    "strconv"
)

// GetPort returns the port on which the server should listen.
// It reads the PORT environment variable, falling back to 8080 if unset or invalid.
func GetPort() string {
    portStr := os.Getenv("PORT")
    if portStr == "" {
        return ":8080"
    }
    // Ensure it's a valid integer.
    if _, err := strconv.Atoi(portStr); err != nil {
        return ":8080"
    }
    // Ensure it starts with ':' as required by gin.Run.
    if portStr[0] != ':' {
        return ":" + portStr
    }
    return portStr
}
