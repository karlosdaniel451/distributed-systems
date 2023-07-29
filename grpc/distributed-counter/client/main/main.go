package main

import (
	"context"
	"distributed-counter/protobuf"
	"fmt"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	serverHost string = "localhost"
	serverPort int    = 9000
)

func main() {
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:%d", serverHost, serverPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("error: failed to connect to %s:%d: %s", serverHost, serverPort, err)
	}
	defer conn.Close()

	ctx := context.Background()

	counterService := protobuf.NewDistributedCounterServiceClient(conn)

	counter, err := counterService.GetCounter(ctx, &protobuf.GetCounterRequest{})
	if err != nil {
		log.Fatalf("error when calling gRPC server: %s", err)
	}

	fmt.Printf("counter value: %d\n", counter.GetValue())

	numberOfIncrementers := 1_000
	numberOfDecrementers := 1_000

	var wg sync.WaitGroup

	for i := 0; i < numberOfIncrementers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println("incrementing counter...")
			counter, err := counterService.Increment(ctx, &protobuf.IncrementRequest{})
			if err != nil {
				log.Fatalf("error when calling gRPC server: %s", err)
			}

			fmt.Printf("counter value: %d\n", counter.GetValue())
		}()
	}

	for i := 0; i < numberOfDecrementers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println("decrementing counter...")
			counter, err := counterService.Decrement(ctx, &protobuf.DecrementRequest{})
			if err != nil {
				log.Fatalf("error when calling gRPC server: %s", err)
			}

			fmt.Printf("counter value: %d\n", counter.GetValue())
		}()
	}

	wg.Wait()

	counter, err = counterService.GetCounter(ctx, &protobuf.GetCounterRequest{})
	if err != nil {
		log.Fatalf("error when calling gRPC server: %s", err)
	}

	fmt.Printf("final counter value: %d\n", counter.GetValue())
}
