package store

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
)

const logFileName = "fstore.log"
const logEntryFormat = "%d,%d,%s,%s"

type fileLogStore struct {
	inMemoryStore Store
	lastSeq       uint64
	file          *os.File

	done  chan struct{}
	tasks chan eventTask
}

type eventType byte

const (
	eventPut eventType = iota + 1
	eventDelete
)

type event struct {
	seqId     uint64    // A unique record ID
	eventType eventType // The action taken
	key       string    // The key affected by this transaction
	value     string    // The value of a PUT the transaction
}

type eventTask struct {
	e event

	cSuccess chan struct{}
	cError   chan error
}

/*
 * Performance:
 *   * GET    4-15 µs
 *   * PUT    9-29 ms
 *   * DELETE 9-28 ms
 */
func NewFileLogStore() (Store, error) {
	f, err := os.OpenFile(logFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open log file: %w", err)
	}

	s := fileLogStore{
		inMemoryStore: NewSyncInMemoryStore(),
		file:          f,

		tasks: make(chan eventTask),
		done:  make(chan struct{}),
	}

	if err = s.init(); err != nil {
		return nil, err
	}

	go s.run()

	return &s, nil
}

func (s *fileLogStore) Get(key string) (string, bool, error) {
	return s.inMemoryStore.Get(key)
}

func (s *fileLogStore) Put(key string, value string) error {
	t := eventTask{
		e:        event{eventType: eventPut, key: key, value: value},
		cSuccess: make(chan struct{}, 1),
		cError:   make(chan error, 1),
	}

	s.tasks <- t

	select {
	case <-t.cSuccess:
		return nil
	case err := <-t.cError:
		return err
	}
}

func (s *fileLogStore) Delete(key string) error {
	t := eventTask{
		e:        event{eventType: eventDelete, key: key},
		cSuccess: make(chan struct{}, 1),
		cError:   make(chan error, 1),
	}

	s.tasks <- t

	select {
	case <-t.cSuccess:
		return nil
	case err := <-t.cError:
		return err
	}
}

func (s *fileLogStore) Done() error {
	log.Println("shutting down WAL store")
	s.done <- struct{}{}
	return nil
}

func (s *fileLogStore) init() error {
	log.Default().Println("starting log store initialization...")
	stat, err := s.file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		log.Default().Println("log was empty")
		return nil
	}

	log.Default().Println("reading log and reducing it to the memory store")

	var e event

	scanner := bufio.NewScanner(s.file)
	for scanner.Scan() {
		line := scanner.Text()

		if n, err := fmt.Sscanf(line, logEntryFormat, &e.seqId, &e.eventType, &e.key, &e.value); err != nil && n < 3 {
			return fmt.Errorf("error while reading log entries: %w\n", err)
		}

		if s.lastSeq >= e.seqId {
			return errors.New("events in disk are out of order")
		}

		s.lastSeq = e.seqId
		if err = s.applyEventToInMemoryStore(e); err != nil {
			return fmt.Errorf("error while applying events: %w\n", err)
		}
	}

	if err = scanner.Err(); err != nil {
		return fmt.Errorf("error while scanning log file: %w\n", err)
	}

	log.Println("log initialization successful")

	return nil
}

func (s *fileLogStore) run() {
	defer func() {
		if err := s.file.Sync(); err != nil {
			log.Printf("could not fsync log file: %v\n", err)
		}

		if err := s.file.Close(); err != nil {
			log.Printf("could not close log file: %v\n", err)
		}
		log.Println("log file closed successfully")
	}()

	for {
		select {
		case t := <-s.tasks:
			seq := s.lastSeq + 1
			_, err := fmt.Fprintf(s.file, logEntryFormat+"\n", seq, t.e.eventType, t.e.key, t.e.value)
			if err != nil {
				t.cError <- err
				continue
			}

			//err = s.file.Sync()
			//if err != nil {
			//	t.cError <- err
			//	continue
			//}

			s.lastSeq = seq

			if err = s.applyEventToInMemoryStore(t.e); err != nil {
				t.cError <- err
				return
			}

			t.cSuccess <- struct{}{}
		case <-s.done:
			log.Printf("stoped WAL store daemon")
			return
		}
	}
}

func (s *fileLogStore) applyEventToInMemoryStore(e event) error {
	var err error

	switch e.eventType {
	case eventPut:
		err = s.inMemoryStore.Put(e.key, e.key)
	case eventDelete:
		err = s.inMemoryStore.Delete(e.key)
	}

	return err
}
