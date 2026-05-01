package rabbitmq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeConn simulates an amqp.Connection for unit tests.
// Closing notifyCh simulates a connection drop.
type fakeConn struct {
	notifyCh chan *amqp.Error
}

func (f *fakeConn) Channel() (*amqp.Channel, error) { return nil, nil }
func (f *fakeConn) Close() error                    { return nil }
func (f *fakeConn) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	go func() {
		for err := range f.notifyCh {
			ch <- err
		}
		close(ch)
	}()
	return ch
}

func TestResilientConn_ReconnectsAfterDrop(t *testing.T) {
	dialed := make(chan struct{}, 10)
	firstDrop := make(chan *amqp.Error)

	var calls atomic.Int32
	dialFn := func(_ string) (Conn, error) {
		dialed <- struct{}{}
		if calls.Add(1) == 1 {
			return &fakeConn{notifyCh: firstDrop}, nil
		}
		return &fakeConn{notifyCh: make(chan *amqp.Error)}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rc := newResilientConn("amqp://fake", dialFn)
	go rc.reconnectLoop(ctx)

	select {
	case <-dialed:
	case <-time.After(time.Second):
		t.Fatal("initial dial did not happen")
	}

	close(firstDrop) // simulate connection drop

	select {
	case <-dialed:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect did not happen after connection drop")
	}
}

func TestResilientConn_StopsOnContextCancel(t *testing.T) {
	dialed := make(chan struct{}, 1)
	dialFn := func(_ string) (Conn, error) {
		dialed <- struct{}{}
		return &fakeConn{notifyCh: make(chan *amqp.Error)}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	rc := newResilientConn("amqp://fake", dialFn)
	done := make(chan struct{})
	go func() {
		rc.reconnectLoop(ctx)
		close(done)
	}()

	<-dialed
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnectLoop did not stop after context cancel")
	}
}

func TestResilientConn_ReadyClosedAfterFirstDial(t *testing.T) {
	dialFn := func(_ string) (Conn, error) {
		return &fakeConn{notifyCh: make(chan *amqp.Error)}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rc := newResilientConn("amqp://fake", dialFn)
	go rc.reconnectLoop(ctx)

	select {
	case <-rc.ready:
	case <-time.After(time.Second):
		t.Fatal("ready channel was not closed after first dial")
	}
}
