package server

import (
	"context"
	"distributed-counter/protobuf"
	"sync"
)

type DistributedCounterService struct {
	protobuf.UnimplementedDistributedCounterServiceServer
	counter protobuf.Counter
	mu sync.RWMutex
}

// Increment the counter value by 1 and return its value after such operation.
func (service *DistributedCounterService) Increment(
	ctx context.Context, request *protobuf.IncrementRequest,
) (*protobuf.Counter, error) {

	service.mu.Lock()
	service.counter.Value = service.counter.GetValue() + 1
	defer service.mu.Unlock()

	return &protobuf.Counter{Value: service.counter.GetValue()}, nil
}

// Decrement the counter value by 1 and return its value after such operation.
func (service *DistributedCounterService) Decrement(
	ctx context.Context, request *protobuf.DecrementRequest,
) (*protobuf.Counter, error) {

	service.mu.Lock()
	service.counter.Value = service.counter.GetValue() - 1
	defer service.mu.Unlock()

	return &protobuf.Counter{Value: service.counter.GetValue()}, nil
}

// Return the counter value.
func (service *DistributedCounterService) GetCounter(
	ctx context.Context, request *protobuf.GetCounterRequest,
) (*protobuf.Counter, error) {

	service.mu.RLock()
	defer service.mu.RUnlock()

	return &protobuf.Counter{Value: service.counter.GetValue()}, nil
}
