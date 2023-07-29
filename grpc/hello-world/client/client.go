package main

import (
	"context"
	"fmt"
	"hello-world/protobuf"
	"log"
	"sync"
	"time"

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

	grpcClient := protobuf.NewHelloServiceClient(conn)

	helloRequest := protobuf.HelloRequest{Greeting: "Hello Server"}

	const numberOfRPCsToCall = 10_000
	start := time.Now().UTC()

	// Send many RPCs
	for i := 0; i < numberOfRPCsToCall; i++ {
		_, err := grpcClient.SayHello(ctx, &helloRequest)
		if err != nil {
			log.Fatalf("error when calling gRPC server: %s", err)
		}
	}

	timeToHandleManyRequests := time.Since(start)
	fmt.Printf("time to handle %d RPCs sending them synchronously: %v\n",
		numberOfRPCsToCall, timeToHandleManyRequests)

	start = time.Now().UTC()
	var wg sync.WaitGroup

	// Send many RPCs
	for i := 0; i < numberOfRPCsToCall; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := grpcClient.SayHello(ctx, &helloRequest)
			if err != nil {
				log.Fatalf("error when calling gRPC server: %s", err)
			}
		}()
	}
	wg.Wait()

	timeToHandleManyRequests = time.Since(start)
	fmt.Printf("time to handle %d RPCs sending them asynchronously: %v\n",
		numberOfRPCsToCall, timeToHandleManyRequests)

	const timeout time.Duration = time.Second * 1
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Cancel the sending of RPCs after `timeout` passes
	time.AfterFunc(timeout, func() {
		cancel()
		log.Print("cancelling sending of RPCs")
	})

	var wg2 sync.WaitGroup

	numberOfHandledRPCs := 0
	loopCond := true
	for loopCond {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			_, err = grpcClient.SayHello(ctx, &helloRequest)
			if err != nil {
				loopCond = false
				return
			}
			numberOfHandledRPCs++
		}()
	}
	wg2.Wait()

	fmt.Printf("numberOfHandledRPCs in %v: %d\n", timeout, numberOfHandledRPCs)
}
