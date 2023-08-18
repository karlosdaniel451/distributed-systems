package main

import (
	"log"

	"github.com/nats-io/nats.go"
)

func main() {
	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("error when connecting to %s: %s", nats.DefaultURL, err)
	}

	defer conn.Drain()

	err = conn.Publish("hello_world", []byte("hello pub-sub"))
	if err != nil {
		log.Fatalf("error when publishing message: %s", err)
	}

}
