package main

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("error when connecting to %s: %s", nats.DefaultURL, err)
	}

	defer conn.Drain()

	subject := "help"
	requestData := "Need some help"

	ctx, cancel := context.WithTimeout(context.Background(), 3 * time.Second)
	defer cancel()

	// Try to send request and receive request
	response, err := conn.RequestWithContext(ctx, subject, []byte(requestData))
	if err != nil {
		log.Fatalf("error when requesting responders: %s", err)
	}

	log.Printf("sent request: %s", requestData)
	log.Printf("response: %s", response.Data)
}
