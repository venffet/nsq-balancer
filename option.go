package balancer

import (
	"time"
)

// Options custom balancer options
type Options struct {
	Addr string

	// Check interval, min 100ms, defaults to 1s
	CheckInterval time.Duration

	// Rise and Fall indicate the number of checks required to
	// mark the instance as up or down, defaults to 1
	Rise, Fall int
}

func (o *Options) getCheckInterval() time.Duration {
	if o.CheckInterval == 0 {
		return time.Second
	} else if o.CheckInterval < minCheckInterval {
		return minCheckInterval
	}
	return o.CheckInterval
}

func (o *Options) getRise() int {
	if o.Rise < 1 {
		return 1
	}
	return o.Rise
}

func (o *Options) getFall() int {
	if o.Fall < 1 {
		return 1
	}
	return o.Fall
}
