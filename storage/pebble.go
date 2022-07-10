package storage

import (
	"context"
	"fmt"
	"path"

	"github.com/cockroachdb/pebble"
)

var _ Storage = (*PebbleWrapper)(nil)

type PebbleWrapper struct {
	DataDir string

	db *pebble.DB
}

func (p *PebbleWrapper) Init() error {
	dir := path.Join(p.DataDir, "pebble")

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return fmt.Errorf("opening db: %w", err)
	}

	p.db = db
	return nil
}

func (p *PebbleWrapper) Get(ctx context.Context, key string) (string, error) {
	data, c, err := p.db.Get([]byte(key))
	if err != nil {
		return "", fmt.Errorf("pebble get: %w", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			panic(err) // TODO: until stable
		}
	}()

	return string(data), nil
}

func (p *PebbleWrapper) Set(ctx context.Context, key, value string) error {
	if err := p.db.Set([]byte(key), []byte(value), &pebble.WriteOptions{}); err != nil {
		return fmt.Errorf("pebble set: %w", err)
	}

	return nil
}

func (p *PebbleWrapper) Snapshot(ctx context.Context) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (p *PebbleWrapper) Close() {
	if err := p.db.Close(); err != nil {
		panic(err) // TODO: until stable
	}
}
