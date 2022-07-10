package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

var _ Storage = (*inMemory)(nil)

type inMemory struct {
	mu sync.Mutex

	storage map[string]string
}

func (i *inMemory) Init() error {
	i.storage = make(map[string]string, 128)
	return nil
}

func (i *inMemory) Get(ctx context.Context, key string) (string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if val, ok := i.storage[key]; ok {
		return val, nil
	} else {
		return "", ErrNotFound
	}
}

func (i *inMemory) Set(ctx context.Context, key, value string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.storage[key] = value

	return nil
}

func (i *inMemory) Snapshot(ctx context.Context) (string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	bytes, err := json.Marshal(i.storage)
	if err != nil {
		return "", fmt.Errorf("serializing snapshot: %w", err)
	}

	return string(bytes), nil
}

func (i *inMemory) Close() {
	i.storage = nil
}
