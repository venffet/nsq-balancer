package balancer

import (
	"math"
	"math/rand"
	"time"
)

type backender interface {
	// Up returns true if up
	Up() bool
	// Down returns true if down
	Down() bool
	// Connections returns the number of connections
	Connections() int64
	// Latency returns the current latency
	Latency() time.Duration
	// Close shuts down the backend
	Close() error
	// Opt returns the options
	Option() *Options
}

type pool []backender

// Up returns all backends that are up
func (p pool) Up() pool {
	return p.all(func(b backender) bool { return b.Up() })
}

// FirstUp returns the first backend that is up
func (p pool) FirstUp() backender {
	return p.first(func(b backender) bool { return b.Up() })
}

// MinUp returns the backend with the minumum result that is up
func (p pool) MinUp(minimum func(backender) int64) backender {
	min := int64(math.MaxInt64)
	pos := -1
	for n, b := range p {
		if b.Up() {
			if num := minimum(b); num < min {
				pos, min = n, num
			}
		}
	}

	if pos < 0 {
		return nil
	}
	return p[pos]
}

// Random returns a random backend
func (p pool) Random() backender {
	if size := len(p); size > 0 {
		return p[rand.Intn(size)]
	}
	return nil
}

// At picks a pool item using at pos (seed)
func (p pool) At(pos int) backender {
	n := len(p)
	if n < 1 {
		return nil
	}
	if pos %= n; pos < 0 {
		pos *= -1
	}
	return p[pos]
}

// WeightedRandom returns a weighted-random backend
func (p pool) WeightedRandom(weight func(backender) int64) backender {
	if len(p) < 1 {
		return nil
	}

	var min, max int64 = math.MaxInt64, 0
	weights := make([]int64, len(p))
	for n, b := range p {
		w := weight(b)
		if w > max {
			max = w
		}
		if w < min {
			min = w
		}
		weights[n] = w
	}

	var sum int64
	for n, w := range weights {
		w = min + max - w
		sum = sum + w
		weights[n] = w
	}

	mark := rand.Int63n(sum)
	for n, w := range weights {
		if mark -= w; mark <= 0 {
			return p[n]
		}
	}

	// We should never reach this point if the slice wasn't empty
	return nil
}

// selects all backends given a criteria
func (p pool) all(criteria func(backender) bool) pool {
	res := make(pool, 0, len(p))
	for _, b := range p {
		if criteria(b) {
			res = append(res, b)
		}
	}
	return res
}

// returns the first matching backend given a criteria, or nil when nothing matches
func (p pool) first(criteria func(backender) bool) backender {
	for _, b := range p {
		if criteria(b) {
			return b
		}
	}
	return nil
}
