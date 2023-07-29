package main

import (
	"distributed-counter/protobuf"
	"distributed-counter/server"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)

const (
	serverHost string = "localhost"
	serverPort int    = 9000
)

func main() {
	var _ protobuf.DistributedCounterServiceServer = &server.DistributedCounterService{}

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", serverHost, serverPort))
	if err != nil {
		log.Fatalf("error: failed to listen to %s:%d: %s", serverHost, serverPort, err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	counterService := server.DistributedCounterService{}
	protobuf.RegisterDistributedCounterServiceServer(grpcServer, &counterService)

	log.Printf("starting gRPC server at: %s\n", listener.Addr())
	if err = grpcServer.Serve(listener); err != nil {
		log.Fatalf("error: failed to start gRPC server at %s: %s", listener.Addr(), err)
	}
}
