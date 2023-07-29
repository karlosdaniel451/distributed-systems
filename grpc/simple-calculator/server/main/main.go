package main

import (
	"fmt"
	"log"
	"net"
	"simple-calculator/protobuf"
	"simple-calculator/server"

	"google.golang.org/grpc"
)

const (
	serverHost string = "localhost"
	serverPort int    = 9000
)

func main() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", serverHost, serverPort))
	if err != nil {
		log.Fatalf("error: failed to listen to %s:%d: %s", serverHost, serverPort, err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	calculatorService := server.CalculatorService{}
	protobuf.RegisterCalculatorServiceServer(grpcServer, &calculatorService)

	log.Printf("starting gRPC server at: %s\n", listener.Addr())
	if err = grpcServer.Serve(listener); err != nil {
		log.Fatalf("error: failed to start gRPC server at %s: %s", listener.Addr(), err) 
	}
}
