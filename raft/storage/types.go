package storage

import pb "github.com/faustuzas/distributed-kv/raft/raftpb"

// StateStorage provides a way to access last persisted state of the node
type StateStorage interface {

	// State tries to return the last persisted state and a flag indicating whether there was one persisted at all
	State() (pb.PersistentState, bool, error)
}

// MutableStateStorage is an extension to StateStorage which provides mutating operations too
type MutableStateStorage interface {
	StateStorage

	// SetState persists the state
	SetState(state pb.PersistentState) error
}

// LogStorage provides a way to access already stable entries
type LogStorage interface {

	// Entries returns a slice of persisted raft log entries in the range [startIdx, endIdx)
	Entries(startIdx, endIdx uint64) ([]pb.Entry, error)

	// Term returns the term of the log entry at the raft log idx
	Term(idx uint64) (uint64, error)

	// LastIndex returns the raft index of the last entry in the log
	LastIndex() (uint64, error)
}

// MutableLogStorage is an extension to LogStorage which provides mutating operations too
type MutableLogStorage interface {
	LogStorage

	// Append persist the entries keeping the Raft index continuity and monotonicity invariants in place
	Append(entries ...pb.Entry) error
}
