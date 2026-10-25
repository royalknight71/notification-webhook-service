package observability

import (
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestErrorSummaryRedactsRequestURLAndBoundsLongErrors(t *testing.T) {
	err := &url.Error{
		Op:  "Post",
		URL: "http://user:secret@example.test/path?token=private",
		Err: errors.New(strings.Repeat("x", maxErrorSummaryBytes+30)),
	}
	summary := ErrorSummary(err)
	if strings.Contains(summary, "secret") || strings.Contains(summary, "private") {
		t.Fatalf("summary exposed request URL credentials: %q", summary)
	}
	if len(summary) != maxErrorSummaryBytes+3 {
		t.Fatalf("summary length=%d want=%d", len(summary), maxErrorSummaryBytes+3)
	}
}
