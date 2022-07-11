package node

import (
	"context"
	"encoding/json"
	"github.com/faustuzas/fstore/raft"
	pb "github.com/faustuzas/fstore/raft/raftpb"
)

func (n *DBNode) runRaftProcess() {
	var prevHardState pb.PersistentState

	for {
		select {
		case progress := <-n.RaftNode.Progress():
			var err error

			if !raft.ArePersistentStatesEqual(prevHardState, progress.HardState) {
				n.Metrics.ObserveRaftSetState(func() {
					err = n.RaftStateStorage.SetState(progress.HardState)
				})
				if err != nil {
					panic(err)
				}
				prevHardState = progress.HardState
			}

			n.Metrics.ObserveRaftAppend(func() {
				err = n.RaftLogStorage.Append(progress.EntriesToPersist...)
			})
			if err != nil {
				panic(err)
			}

			n.RaftTransport.Send(progress.Messages...)

			n.RaftNode.Advance()

			for _, entry := range progress.EntriesToApply {
				if len(entry.Data) == 0 {
					continue
				}

				var req RaftRequest
				_ = json.Unmarshal(entry.Data, &req)

				_ = n.Storage.Set(context.Background(), req.Key, req.Value)
				n.reqWaiter.Trigger(req.ID, req)
			}
		}
	}
}
