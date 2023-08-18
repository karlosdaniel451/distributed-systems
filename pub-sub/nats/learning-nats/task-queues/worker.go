package main

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// func handler(conn *nats.EncodedConn, subject, reply string, task *Task) {
// 	log.Printf("received task with command: %s", task.Command)
// 	taskResult := TaskResult{Result: time.Now().String()}

// 	err := conn.Publish(subject, taskResult)
// 	if err != nil {
// 		log.Fatalf("error when replying request: %s", err)
// 	}
// }

func main() {
	// natsConn, err := nats.Connect(nats.DefaultURL)
	// if err != nil {
	// 	log.Fatalf("error when connecting to %s: %s", nats.DefaultURL, err)
	// }

	// defer natsConn.Drain()

	// conn, err := nats.NewEncodedConn(natsConn, nats.DEFAULT_ENCODER)
	// if err != nil {
	// 	log.Fatalf("error when connecting to %s: %s", nats.DefaultURL, err)
	// }

	// defer conn.Drain()

	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("error when connecting to %s: %s", nats.DefaultURL, err)
	}

	defer conn.Drain()

	subject := "timer"
	queue := "timer_workers"

	// Subscribe to a queue on the subject
	sub, err := conn.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		responseData := time.Now().UTC().String()
		err := conn.Publish(msg.Reply, []byte(responseData))
		if err != nil {
			log.Printf("error when replying request: %s", err)
			return
		}
		log.Printf("sent response: %s", responseData)
	})

	if err != nil {
		log.Fatalf(
			"error when subscribing to subject %s and joining queue %s: %s",
			subject, queue, err,
		)
	}

	log.Printf("subscribed to subject: %s", sub.Subject)

	// Wait forever
	select {}
}
