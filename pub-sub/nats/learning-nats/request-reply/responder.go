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

	subject := "help"

	sub, err := conn.SubscribeSync(subject)
	if err != nil {
		log.Fatalf("error when subscribing to subject %s: %s", subject, err)
	}

	log.Printf("subscribed to subject: %s", sub.Subject)

	ctx := context.Background()

	var responseData string
	for {
		// Try to receive request
		requestMsg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			log.Printf("error when connecting to %s: %s", nats.DefaultURL, err)
			continue
		}

		log.Printf("received request: %s", requestMsg.Data)

		responseData = "Here's the help"

		// Try to send reply
		err = conn.Publish(requestMsg.Reply, []byte(responseData))
		if err != nil {
			log.Printf("error when replying request: %s", err)
			continue
		}

		log.Printf("sent response: %s", responseData)
	}
}
