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

	subject := "hello_world"
	messageData := "hello pub-sub"

	err = conn.Publish(subject, []byte(messageData))
	if err != nil {
		log.Fatal("error when publishing message: %s", err)
	}

	log.Printf("published to subject %s: %s", subject, messageData)
}
