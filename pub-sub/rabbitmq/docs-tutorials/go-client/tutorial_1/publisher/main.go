package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	amqpHost string = "localhost"
	amqpPort string = "5672"
	amqpUser string = "guest"
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

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	messageBody := "hello world"

	err = ch.PublishWithContext(ctx, "", queue.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body: []byte(messageBody),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Print("Hello world message sent")
}
