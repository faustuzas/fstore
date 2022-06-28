package storage

import (
	"fmt"
	"github.com/gogo/protobuf/jsonpb"
	"io"
	"os"
	"path"
	"sync"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
)

const (
	_stateFileName = "state.json"
)

var jsonpbMarshaler = jsonpb.Marshaler{EnumsAsInts: true}

// hints to compiler what interfaces have to be implemented
var (
	_ MutableLogStorage   = (*DiskStorage)(nil)
	_ MutableStateStorage = (*DiskStorage)(nil)
)

type logBlock struct {
	id                int
	startIdx, lastIdx uint64
	size              int
}

func (lb logBlock) firstIndex() (uint64, error) {
	return lb.startIdx, nil
}

func (lb logBlock) lastIndex() (uint64, error) {
	return lb.lastIdx, nil
}

func (lb logBlock) appendEntries(entries ...pb.Entry) error {
	// TODO: implement
	return nil
}

type log []logBlock

func (l log) lastBlock() logBlock {
	return l[len(l)-1]
}

func (l log) lastIndex() (uint64, error) {
	if len(l) == 0 {
		return 0, nil
	}

	return l.lastBlock().lastIndex()
}

func (l log) appendEntries(entries ...pb.Entry) error {
	block := l.lastBlock()
	if err := block.appendEntries(entries...); err != nil {
		return err
	}

	// TODO: check if the block is not full yet and we don't need another one

	return nil
}

type DiskStorage struct {
	mu sync.Mutex

	dataDir string

	log log
}

func NewDiskStorage(dataDir string) *DiskStorage {
	return &DiskStorage{dataDir: dataDir}
}

func (s *DiskStorage) State() (pb.PersistentState, bool, error) {
	f, err := os.Open(s.stateFilePath())
	if err != nil {
		if os.IsNotExist(err) {
			return pb.PersistentState{}, false, nil
		}

		return pb.PersistentState{}, false, fmt.Errorf("opening state file: %w", err)
	}

	var state pb.PersistentState
	err = jsonpb.Unmarshal(f, &state)
	if err != nil {
		return pb.PersistentState{}, false, fmt.Errorf("reading state file: %w", err)
	}

	return state, true, nil
}

func (s *DiskStorage) SetState(state pb.PersistentState) error {
	tmpFile, err := os.CreateTemp(s.dataDir, "*.tmp.json")
	if err != nil {
		return fmt.Errorf("creating tmp file: %w", err)
	}
	defer func() {
		_ = os.Remove(tmpFile.Name())
	}()

	if err = jsonpbMarshaler.Marshal(tmpFile, &state); err != nil {
		return fmt.Errorf("writing to tmp file: %w", err)
	}

	if err = tmpFile.Sync(); err != nil {
		return fmt.Errorf("syncing tmp file: %w", err)
	}

	if err = os.Rename(tmpFile.Name(), s.stateFilePath()); err != nil {
		return fmt.Errorf("attomicaly changing contents of the file: %w", err)
	}

	// fsync the parent directory too to make sure newly created file is really persisted
	// ref: https://lwn.net/Articles/457667/
	dir, err := os.Open(s.dataDir)
	if err != nil {
		return fmt.Errorf("opening data dir: %w", err)
	}

	if err = dir.Sync(); err != nil {
		return fmt.Errorf("syncing data dir: %w", err)
	}

	return nil
}

func (s *DiskStorage) stateFilePath() string {
	return path.Join(s.dataDir, _stateFileName)
}

func (s *DiskStorage) Entries(startIdx, endIdx uint64) ([]pb.Entry, error) {
	//TODO implement me
	panic("implement me")
}

func (s *DiskStorage) Term(idx uint64) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (s *DiskStorage) LastIndex() (uint64, error) {
	return s.log.lastIndex()
}

func (s *DiskStorage) Append(entries ...pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	// check what is the last index and term of the entries stored in disk
	//  * if appended entries nicely follows it, just append to the file and check whether we need to seal the file
	// if we have overlapping entries
	//  * find the offset where they are different,
	//  * truncate everything from there forward
	//  * append new entries to the end

	lastIndex, err := s.log.lastIndex()
	if err != nil {
		return fmt.Errorf("getting last index of the log: %w", err)
	}

	if lastIndex != entries[0].Index {
		panic("not implemented")
	}

	return s.log.appendEntries(entries...)
}

type Encoder interface {
	Encode(w io.Writer, entry interface{}) error
	Decode(r io.Reader, entry interface{}) error
}
