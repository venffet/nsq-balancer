package balancer

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
	"gopkg.in/tomb.v2"
)

// Nsq backend
type nsqBackend struct {
	client *nsq.Producer
	opt    *Options

	up, successes, failures int32
	connections, latency    int64

	closer tomb.Tomb
}

// GetClient getter for property "client"
func (b *nsqBackend) GetClient() *nsq.Producer { return b.client }

func newNsqBackend(opt *Options) *nsqBackend {
	client, err := nsq.NewProducer(opt.Addr, nsq.NewConfig())
	if err != nil {
		fmt.Printf("failed to new nsq producer: %s\n", err)
	}
	backend := &nsqBackend{
		client: client,
		opt:    opt,
		up:     1,

		connections: 1e6,
		latency:     int64(time.Minute),
	}
	backend.startLoop()
	return backend
}

// Up returns true if up
func (b *nsqBackend) Up() bool { return atomic.LoadInt32(&b.up) > 0 }

// Down returns true if down
func (b *nsqBackend) Down() bool { return !b.Up() }

// Connections returns the number of connections
func (b *nsqBackend) Connections() int64 { return atomic.LoadInt64(&b.connections) }

// Latency returns the current latency
func (b *nsqBackend) Latency() time.Duration { return time.Duration(atomic.LoadInt64(&b.latency)) }

// Close shuts down the backend
func (b *nsqBackend) Close() error {
	b.closer.Kill(nil)
	return b.closer.Wait()
}

// Opt returns the options
func (b *nsqBackend) Option() *Options {
	return b.opt
}

func (b *nsqBackend) ping() {
	start := time.Now()
	err := b.client.Ping()
	if err != nil {
		b.updateStatus(false)
		return
	}
	atomic.StoreInt64(&b.latency, int64(time.Now().Sub(start)))

	atomic.StoreInt64(&b.connections, 1)

	b.updateStatus(true)
}

func (b *nsqBackend) incConnections(n int64) {
	atomic.AddInt64(&b.connections, n)
}

func (b *nsqBackend) updateStatus(success bool) {
	if success {
		atomic.StoreInt32(&b.failures, 0)
		rise := b.opt.getRise()

		if n := int(atomic.AddInt32(&b.successes, 1)); n > rise {
			atomic.AddInt32(&b.successes, -1)
		} else if n == rise {
			atomic.CompareAndSwapInt32(&b.up, 0, 1)
		}
	} else {
		atomic.StoreInt32(&b.successes, 0)
		fall := b.opt.getFall()

		if n := int(atomic.AddInt32(&b.failures, 1)); n > fall {
			atomic.AddInt32(&b.failures, -1)
		} else if n == fall {
			atomic.CompareAndSwapInt32(&b.up, 1, 0)
		}
	}
}

func (b *nsqBackend) startLoop() {
	interval := b.opt.getCheckInterval()
	b.ping()

	b.closer.Go(func() error {
		for {
			select {
			case <-b.closer.Dying():
				b.client.Stop()
				return nil
			case <-time.After(interval):
				b.ping()
			}
		}
		return nil
	})
}
