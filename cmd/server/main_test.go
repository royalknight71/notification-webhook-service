package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestObserveConsumerReportsUnexpectedStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	failed := make(chan struct{})
	observed := observeWorker(ctx, done, func() { close(failed) })
	close(done)
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("unexpected consumer stop was not reported")
	}
	select {
	case <-failed:
	default:
		t.Fatal("unexpected consumer stop callback was not called")
	}
}

func TestShutdownServerDrainsRequestsAndCurrentConsumerWork(t *testing.T) {
	handlerStarted := make(chan struct{})
	finishHandler := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		<-finishHandler
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	shutdownStarted := make(chan struct{})
	server.Config.RegisterOnShutdown(func() { close(shutdownStarted) })

	requestDone := make(chan error, 1)
	go func() {
		resp, err := server.Client().Get(server.URL)
		if err == nil {
			_ = resp.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("in-flight HTTP request did not start")
	}

	stopConsumer := make(chan struct{})
	finishConsumer := make(chan struct{})
	consumerDone := make(chan struct{})
	consumerStopped := make(chan struct{})
	go func() {
		<-stopConsumer
		close(consumerStopped)
		<-finishConsumer
		close(consumerDone)
	}()
	dispatcherDone := make(chan struct{})
	backlogDone := make(chan struct{})
	dispatcherCanceled := make(chan struct{})
	serviceCtx, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	shutdownResult := make(chan error, 1)
	go func() {
		shutdownResult <- shutdownServer(server.Config, func() { close(stopConsumer) }, func() {
			close(dispatcherCanceled)
			close(dispatcherDone)
			close(backlogDone)
		}, cancelService, consumerDone, dispatcherDone, backlogDone, shutdownTimeouts{
			http: time.Second, consumer: time.Second, worker: time.Second,
		})
	}()
	select {
	case <-shutdownStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP shutdown did not begin")
	}
	select {
	case err := <-shutdownResult:
		t.Fatalf("shutdown returned before in-flight request finished: %v", err)
	default:
	}
	close(finishHandler)
	select {
	case err := <-requestDone:
		if err != nil {
			t.Fatalf("in-flight request failed while draining: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight request did not finish")
	}
	select {
	case <-consumerStopped:
	case <-time.After(time.Second):
		t.Fatal("consumer intake was not stopped after HTTP drain")
	}
	close(finishConsumer)
	select {
	case err := <-shutdownResult:
		if err != nil {
			t.Fatalf("graceful shutdown failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not wait for current consumer work")
	}
	select {
	case <-dispatcherCanceled:
	default:
		t.Fatal("dispatcher was not stopped")
	}
	select {
	case <-backlogDone:
	default:
		t.Fatal("backlog sampler was not stopped")
	}
	if !errors.Is(serviceCtx.Err(), context.Canceled) {
		t.Fatal("service context was not canceled after drain")
	}
}

func TestShutdownServerForceClosesConnectionsAfterTimeout(t *testing.T) {
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		<-r.Context().Done()
		close(handlerCanceled)
	}))
	defer server.Close()
	requestDone := make(chan struct{})
	go func() {
		resp, _ := server.Client().Get(server.URL)
		if resp != nil {
			_ = resp.Body.Close()
		}
		close(requestDone)
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking HTTP request did not start")
	}
	consumerDone, dispatcherDone, backlogDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	serviceCtx, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	result := shutdownServer(server.Config,
		func() { close(consumerDone) },
		func() { close(dispatcherDone); close(backlogDone) },
		cancelService,
		consumerDone,
		dispatcherDone,
		backlogDone,
		shutdownTimeouts{http: 25 * time.Millisecond, consumer: time.Second, worker: time.Second},
	)
	if !errors.Is(result, context.DeadlineExceeded) {
		t.Fatalf("shutdown error=%v, want deadline exceeded", result)
	}
	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("force-close did not cancel the active HTTP request")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("client did not observe forced connection close")
	}
	if !errors.Is(serviceCtx.Err(), context.Canceled) {
		t.Fatal("service context was not canceled")
	}
}

func TestObserveConsumerIgnoresShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	failed := make(chan struct{})
	observed := observeWorker(ctx, done, func() { close(failed) })
	cancel()
	close(done)
	select {
	case <-observed:
		t.Fatal("normal shutdown was reported as consumer failure")
	case <-time.After(20 * time.Millisecond):
	}
}
