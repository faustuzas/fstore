package store

import "sync"

type syncInMemoryStore struct {
	sync.RWMutex
	m map[string]string
}

/*
 * Performance:
 *   * GET    4-15 µs
 *   * PUT    10-28 µs
 *   * DELETE 1.5 µs
 */
func NewSyncInMemoryStore() Store {
	return &syncInMemoryStore{
		m: map[string]string{},
	}
}

func (s *syncInMemoryStore) Get(key string) (string, bool, error) {
	s.RLock()
	defer s.RUnlock()

	value, ok := s.m[key]
	return value, ok, nil
}

func (s *syncInMemoryStore) Put(key string, value string) error {
	s.Lock()
	defer s.Unlock()

	s.m[key] = value

	return nil
}

func (s *syncInMemoryStore) Delete(key string) error {
	s.Lock()
	defer s.Unlock()

	delete(s.m, key)

	return nil
}

func (s *syncInMemoryStore) Done() error {
	return nil
}