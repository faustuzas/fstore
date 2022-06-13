package storage

import (
	"context"
	"fmt"
)

var ErrNotFound = fmt.Errorf("entity not found")

type Storage interface {
	Init() error

	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string) error
	Snapshot(ctx context.Context) (string, error)

	Close()
}

type Params struct{}

func CreateStorage(params Params) (Storage, error) {
	storage := &inMemory{}
	if err := storage.Init(); err != nil {
		return nil, fmt.Errorf("initialising storage: %w", err)
	}

	return storage, nil
}
