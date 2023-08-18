package main

import (
	"log"

	"github.com/nats-io/nats.go"
)

func main() {
	// Try to connect to a NATS server running on the default URL
	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("error when connecting to %s: %s", nats.DefaultURL, err)
	}

	defer conn.Drain()

	subject := "hello_world"

	// Subscribe to a subject and add an on message handler
	subscription, err := conn.Subscribe(subject, func(msg *nats.Msg) {
		log.Printf("received message: %s", msg.Data)
	})

	if err != nil {
		log.Fatalf("error when subscribing to subject %s: s", subject, err)
	}

	log.Printf("subscribed to subject: %s", subscription.Subject)

	// Wait forever
	select {}
}
