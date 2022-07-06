package storage

import (
	"testing"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
)

func TestInMemoryStateStorage(t *testing.T) {
	suite := &StateStorageTestSuite{
		t: t,
		factory: func(state *pb.PersistentState) MutableStateStorage {
			return &InMemory{state: state}
		},
	}

	suite.Run()
}

func TestInMemoryLogStorage(t *testing.T) {
	suite := &LogStorageTestSuite{
		t: t,
		factory: func(entries []pb.Entry) MutableLogStorage {
			return &InMemory{entries: entries}
		},
		allEntries: func(storage MutableLogStorage) []pb.Entry {
			s := storage.(*InMemory)
			return s.entries
		},
	}

	suite.Run()
}
