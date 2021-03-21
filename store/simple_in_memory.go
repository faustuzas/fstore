package store

type simpleInMemoryStore map[string]string

/*
 * Performance:
 *   * GET    2-6 µs
 *   * PUT    13 µs
 *   * DELETE 1.5 µs
 */
func NewInMemoryStore() Store {
	return &simpleInMemoryStore{}
}

func (s *simpleInMemoryStore) Get(key string) (string, bool, error) {
	value, ok := (*s)[key]
	return value, ok, nil
}

func (s *simpleInMemoryStore) Put(key string, value string) error {
	(*s)[key] = value
	return nil
}

func (s *simpleInMemoryStore) Delete(key string) error {
	delete(*s, key)
	return nil
}

func (s *simpleInMemoryStore) Done() error {
	return nil
}