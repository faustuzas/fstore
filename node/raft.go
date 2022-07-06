package node

import (
	"context"
	"encoding/json"
	"github.com/faustuzas/distributed-kv/raft"
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
)

func (n *DBNode) runRaftProcess() {
	var prevHardState pb.PersistentState

	for {
		select {
		case progress := <-n.RaftNode.Progress():
			if !raft.ArePersistentStatesEqual(prevHardState, progress.HardState) {
				if err := n.RaftStateStorage.SetState(progress.HardState); err != nil {
					panic(err)
				}
				prevHardState = progress.HardState
			}

			if err := n.RaftLogStorage.Append(progress.EntriesToPersist...); err != nil {
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
