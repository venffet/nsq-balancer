package balancer

import (
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

// BalanceMode mode
type BalanceMode int

// defined mode
const (
	// LeastConn picks the backend with the fewest connections.
	ModeLeastConn BalanceMode = iota
	// FirstUp always picks the first available backend.
	ModeFirstUp
	// ModeMinLatency always picks the backend with the minimal latency.
	ModeMinLatency
	// ModeRandom selects backends randomly.
	ModeRandom
	// ModeWeightedLatency uses latency as a weight for random selection.
	ModeWeightedLatency
	// ModeRoundRobin round-robins across available backends.
	ModeRoundRobin
)

const minCheckInterval = 100 * time.Millisecond

// Balancer client
type Balancer struct {
	selector pool
	mode     BalanceMode
	cursor   int32
}

// GetSelector Getter for property "selector"
func (b *Balancer) GetSelector() pool { return b.selector }

// New initializes a new nsq balancer
func New(opts []Options, mode BalanceMode) *Balancer {
	if len(opts) == 0 {
		opts = []Options{
			Options{Addr: "127.0.0.1:4150"},
		}
	}

	balancer := &Balancer{
		selector: make(pool, len(opts)),
		mode:     mode,
	}
	for i := 0; i < len(opts); i++ {
		balancer.selector[i] = newNsqBackend(&opts[i])
	}
	return balancer
}

// Next returns the next available nsq client
func (b *Balancer) Next() *nsq.Producer { return b.pickNext().client }

// Close closes all connecitons in the balancer
func (b *Balancer) Close() (err error) {
	for _, b := range b.selector {
		if e := b.Close(); e != nil {
			err = e
		}
	}
	return
}

// Pick the next backend
func (b *Balancer) pickNext() (backend *nsqBackend) {
	var bi backender
	switch b.mode {
	case ModeLeastConn:
		bi = b.selector.MinUp(func(b backender) int64 {
			return b.Connections()
		})
	case ModeFirstUp:
		bi = b.selector.FirstUp()
	case ModeMinLatency:
		bi = b.selector.MinUp(func(b backender) int64 {
			return int64(b.Latency())
		})
	case ModeRandom:
		bi = b.selector.Up().Random()
	case ModeWeightedLatency:
		bi = b.selector.Up().WeightedRandom(func(b backender) int64 {
			factor := int64(b.Latency())
			return factor * factor
		})
	case ModeRoundRobin:
		next := int(atomic.AddInt32(&b.cursor, 1))
		bi = b.selector.Up().At(next)
	}

	// Fall back on random backend
	if bi == nil {
		bi = b.selector.Random()
	}

	backend = bi.(*nsqBackend)

	// Increment the number of connections
	backend.incConnections(1)
	return
}
