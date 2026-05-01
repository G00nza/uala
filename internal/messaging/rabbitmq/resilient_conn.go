package rabbitmq

import (
	"context"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Conn is the minimal interface ResilientConn wraps. *amqp.Connection satisfies it.
type Conn interface {
	Channel() (*amqp.Channel, error)
	NotifyClose(chan *amqp.Error) chan *amqp.Error
	Close() error
}

// channeler is what consumer and publisher need from the connection.
type channeler interface {
	Channel() (*amqp.Channel, error)
}

// ResilientConn wraps an AMQP connection and transparently reconnects on drops.
type ResilientConn struct {
	url    string
	dialFn func(string) (Conn, error)
	mu     sync.RWMutex
	conn   Conn
	ready  chan struct{}
	once   sync.Once
}

func newResilientConn(url string, dialFn func(string) (Conn, error)) *ResilientConn {
	return &ResilientConn{
		url:    url,
		dialFn: dialFn,
		ready:  make(chan struct{}),
	}
}

func (r *ResilientConn) Channel() (*amqp.Channel, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.conn.Channel()
}

func (r *ResilientConn) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *ResilientConn) reconnectLoop(ctx context.Context) {
	backoff := time.Second
	for {
		conn, err := r.dialFn(r.url)
		if err != nil {
			log.Printf("amqp: dial: %v, retry in %s", err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				if backoff < 30*time.Second {
					backoff *= 2
				}
			}
			continue
		}

		r.mu.Lock()
		r.conn = conn
		r.mu.Unlock()
		r.once.Do(func() { close(r.ready) })
		backoff = time.Second

		notifyClose := conn.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case <-ctx.Done():
			return
		case <-notifyClose:
			log.Printf("amqp: connection lost, reconnecting")
		}
	}
}
