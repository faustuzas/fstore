package raft

import (
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
)

// hints to compiler what interfaces have to be implemented
var (
	_ LogStorage   = (*DiskStorage)(nil)
	_ StateStorage = (*DiskStorage)(nil)
)

type DiskStorage struct {
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
	//TODO implement me
	panic("implement me")
}

func (s *DiskStorage) State() (pb.PersistentState, bool, error) {
	//TODO implement me
	panic("implement me")
}

// TODO: load already stored entries in the disk to Raft Node
