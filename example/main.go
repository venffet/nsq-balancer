package main

import (
	"log"
	"time"

	"github.com/venffet/nsq-balancer"
)

func main() {
	clients := balancer.New(
		[]balancer.Options{
			{Addr: "127.0.0.1:6376", CheckInterval: 600 * time.Millisecond},
			{Addr: "127.0.0.1:4150", CheckInterval: 800 * time.Millisecond},
		},
		balancer.ModeRoundRobin,
	)
	defer clients.Close()

	client := clients.Next()
	err := client.Ping()
	if err != nil {
		log.Fatalf("failed to ping: %s", err)
	}
}
