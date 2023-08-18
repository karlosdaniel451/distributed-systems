package main

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
)

func main() {
	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("error when connecting to %s: %s", nats.DefaultURL, err)
	}

	defer conn.Drain()

	subject := "hello_world"

	sub, err := conn.SubscribeSync(subject)
	if err != nil {
		log.Fatalf("error when subscribing to subject %s: s", subject, err)
	}

	log.Printf("subscribed to subject: %s", sub.Subject)

	ctx := context.Background()

	for {
		msg, err  := sub.NextMsgWithContext(ctx)
		if err != nil {
			log.Printf("error when connecting to %s: %s", nats.DefaultURL, err)
			continue
		}

		log.Printf("received message: %s", msg.Data)
	}
}
