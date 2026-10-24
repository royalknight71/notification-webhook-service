package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebhookHandlerAcceptsRawBodyAndHostHeader(t *testing.T) {
	payload := []byte("{\n  \"z\": 1.00, \"a\": 2e+03\n}")
	var output bytes.Buffer
	handler := newWebhookHandler(&output)
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8091/", bytes.NewReader(payload))
	request.Host = "host.docker.internal:8091"
	request.Header.Set("X-Signature", "sha256=test-signature")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d want=%d body=%s", response.Code, http.StatusNoContent, response.Body)
	}
	wantLog := fmt.Sprintf("received webhook host=%q bytes=%d X-Signature=%q body=%q", request.Host, len(payload), "sha256=test-signature", payload)
	if !strings.Contains(output.String(), wantLog) {
		t.Fatalf("receiver output %q does not contain %q", output.String(), wantLog)
	}
}

func TestWebhookHandlerRejectsOversizedBody(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8091/", bytes.NewReader(bytes.Repeat([]byte("x"), maxWebhookBody+1)))
	response := httptest.NewRecorder()
	newWebhookHandler(&bytes.Buffer{}).ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d want=%d", response.Code, http.StatusRequestEntityTooLarge)
	}
}
