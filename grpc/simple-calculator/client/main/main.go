package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"simple-calculator/protobuf"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	serverHost string = "localhost"
	serverPort int    = 9000
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	log.Printf("trying to connect to gRPC server at %s:%d...\n", serverHost, serverPort)
	conn, err := grpc.DialContext(
		ctx,
		fmt.Sprintf("%s:%d", serverHost, serverPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  time.Second * 2,
				Multiplier: 2,
				Jitter:     0.2,
				MaxDelay:   time.Second * 20,
			},
		}),
		grpc.WithDialer(func(s string, d time.Duration) (net.Conn, error) {
			log.Println("connection attempt...")
			return net.DialTimeout("tcp", fmt.Sprintf("%s:%d", serverHost, serverPort), d)
		}),
	)
	if err != nil {
		log.Fatalf("error: failed to connect to gRPC server at %s:%d: %s",
			serverHost, serverPort, err)
	}
	defer conn.Close()

	calculatorService := protobuf.NewCalculatorServiceClient(conn)

	calculatorRequest := &protobuf.CalculatorRequest{}
	result := &protobuf.Result{}

	func() {
		calculatorRequest = &protobuf.CalculatorRequest{
			Operand1: 10.0,
			Operand2: 5.5,
		}

		result, err = calculatorService.Sum(ctx, calculatorRequest)
		if err != nil {
			log.Fatalf("error: failed to call Sum(): %s", grpc.ErrorDesc(err))
		}
		log.Printf("Result value: %f\n", result.GetValue())
	}()

	func() {
		calculatorRequest.Operand2 = 0

		result, err = calculatorService.Divide(ctx, calculatorRequest)
		if err != nil {
			log.Fatalf("error: failed to call Divide(): %s", grpc.ErrorDesc(err))
		}

		log.Printf("Result value: %f\n", result.GetValue())
	}()
}
