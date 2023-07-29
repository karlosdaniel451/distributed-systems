package server

import (
	"context"
	"simple-calculator/protobuf"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type CalculatorService struct {
	protobuf.UnimplementedCalculatorServiceServer
}

func (calc *CalculatorService) Sum(ctx context.Context,
	request *protobuf.CalculatorRequest) (*protobuf.Result, error) {

	return &protobuf.Result{
		Value: request.GetOperand1() + request.GetOperand2(),
	}, nil
}

func (calc *CalculatorService) Subtract(ctx context.Context,
	request *protobuf.CalculatorRequest) (*protobuf.Result, error) {

	return &protobuf.Result{
		Value: request.GetOperand1() - request.GetOperand2(),
	}, nil
}

func (calc *CalculatorService) Multiply(ctx context.Context,
	request *protobuf.CalculatorRequest) (*protobuf.Result, error) {

	return &protobuf.Result{
		Value: request.GetOperand1() * request.GetOperand2(),
	}, nil
}

func (calc *CalculatorService) Divide(ctx context.Context,
	request *protobuf.CalculatorRequest) (*protobuf.Result, error) {

	if request.GetOperand2() == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument,
			"invalid operation: division by zero")
	}

	return &protobuf.Result{
		Value: request.GetOperand1() / request.GetOperand2(),
	}, nil
}