package main

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	natsConn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("error when connecting to %s: %s", nats.DefaultURL, err)
	}

	defer natsConn.Drain()

	conn, err := nats.NewEncodedConn(natsConn, nats.DEFAULT_ENCODER)
	if err != nil {
		log.Fatalf("error when connecting to %s: %s", nats.DefaultURL, err)
	}

	subject := "timer"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// requestData := "now()"
	requestData := ""
	responseData := ""

	// Try to send request and receive response
	err = conn.RequestWithContext(ctx, subject, requestData, &responseData)
	if err != nil {
		log.Fatalf("error when submiting task: %s", err)
	}

	log.Printf("sent task with command: %s", requestData)
	log.Printf("task result: %s", responseData)
}
