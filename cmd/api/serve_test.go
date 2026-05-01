package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServe_ShutsDownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, ln, handler)
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned error after context cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not shut down within 5s after context cancel")
	}
}

func TestServe_CompletesInFlightRequestBeforeShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	requestStarted := make(chan struct{})
	requestDone := make(chan struct{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		close(requestDone)
	})

	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, ln, handler)
	}()

	go func() {
		http.Get("http://" + addr + "/") //nolint:errcheck
	}()

	<-requestStarted
	cancel()

	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request was not completed before shutdown")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not shut down within 5s")
	}
}
