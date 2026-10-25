package observability

import (
	"errors"
	"net/url"
)

const maxErrorSummaryBytes = 256

// ErrorSummary keeps request URLs and long response details out of logs.
func ErrorSummary(err error) string {
	if err == nil {
		return ""
	}
	var requestErr *url.Error
	if errors.As(err, &requestErr) {
		err = requestErr.Err
	}
	message := err.Error()
	if len(message) > maxErrorSummaryBytes {
		message = message[:maxErrorSummaryBytes] + "..."
	}
	return message
}
