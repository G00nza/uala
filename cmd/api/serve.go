package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

const shutdownTimeout = 10 * time.Second

func serve(ctx context.Context, ln net.Listener, handler http.Handler) error {
	srv := &http.Server{Handler: handler}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
