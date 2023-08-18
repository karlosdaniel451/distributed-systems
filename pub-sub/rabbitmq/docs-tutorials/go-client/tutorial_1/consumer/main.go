package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	amqpHost     string = "localhost"
	amqpPort     string = "5672"
	amqpUser     string = "guest"
	amqpPassword string = "guest"
)

func main() {
	conn, err := amqp.Dial(fmt.Sprintf(
		"amqp://%s:%s@%s:%s/",
		amqpUser, amqpPassword, amqpHost, amqpPort,
	))
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare(
		"hello-world-queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	deliveryChannel, err := ch.Consume(
		queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	forever := make(chan struct{})

	go func() {
		for message := range deliveryChannel {
			log.Printf("Received: %s", message.Body)
		}
	}()

	log.Print("Waiting for messages.")
	<-forever
}
