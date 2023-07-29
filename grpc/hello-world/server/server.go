package main

import (
	"context"
	"fmt"
	"hello-world/protobuf"
	"log"
	"net"

	"google.golang.org/grpc"
)

type HelloService struct {
	protobuf.UnimplementedHelloServiceServer
}

func (service *HelloService) SayHello(ctx context.Context,
	request *protobuf.HelloRequest) (*protobuf.HelloResponse, error) {

	// log.Printf("greeting from client: %s\n", request.GetGreeting())

	return &protobuf.HelloResponse{Reply: "Hello Client"}, nil
}

const (
	serverHost string = "localhost"
	serverPort int    = 9000
)

func main() {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", serverHost, serverPort))
	if err != nil {
		log.Fatalf("error: failed to listen %s:%d: %s", serverHost, serverPort, err)
	}

	grpcServer := grpc.NewServer()
	protobuf.RegisterHelloServiceServer(grpcServer, &HelloService{})

	log.Printf("starting gRPC server at %s\n", listener.Addr())
	if err = grpcServer.Serve(listener); err != nil {
		log.Fatalf("error: failed to start gRPC server: %s", err)
	}
}
