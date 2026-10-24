package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

const maxWebhookBody = 1 << 20

func main() {
	const addr = "127.0.0.1:8091"

	server := &http.Server{
		Addr:              addr,
		Handler:           newWebhookHandler(os.Stdout),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()
	log.Printf("local test webhook listening at http://%s/", addr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("test webhook shutdown: %v", err)
		}
	}
}

func newWebhookHandler(output io.Writer) http.Handler {
	logger := log.New(output, "", 0)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody+1))
		if err != nil {
			http.Error(w, "could not read webhook body", http.StatusBadRequest)
			return
		}
		if len(body) > maxWebhookBody {
			http.Error(w, "webhook body too large", http.StatusRequestEntityTooLarge)
			return
		}

		logger.Printf("received webhook host=%q bytes=%d X-Signature=%q body=%q", r.Host, len(body), r.Header.Get("X-Signature"), body)
		w.WriteHeader(http.StatusNoContent)
	})
}
