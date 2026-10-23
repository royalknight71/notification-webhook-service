package main

import (
	"context"
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
