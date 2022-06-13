package main

import (
	"fmt"
	"github.com/faustuzas/distributed-kv/config"
	"github.com/faustuzas/distributed-kv/logging"
	"github.com/faustuzas/distributed-kv/node"
	"github.com/faustuzas/distributed-kv/raft"
	"github.com/faustuzas/distributed-kv/raft/transport"
	"github.com/faustuzas/distributed-kv/storage"
	"github.com/faustuzas/distributed-kv/util"
	"os"
	"runtime"
	"strconv"
)

func runAll() error {
	for _, id := range []int{1, 2, 3} {
		if err := runNode(id); err != nil {
			return fmt.Errorf("starting node %v: %w", id, err)
		}
	}

	return nil
}

func runNode(serverId int) error {
	configFetcher, err := config.NewDefaultFetcher()
	if err != nil {
		return fmt.Errorf("creating config fetcher: %w", err)
	}

	topology, err := configFetcher.FetchTopology()
	if err != nil {
		return fmt.Errorf("fetching topology config: %w", err)
	}

	if err := startNode(config.ServerId(serverId), topology); err != nil {
		return fmt.Errorf("starting node %v: %w", serverId, err)
	}

	return nil
}

func startNode(id config.ServerId, topology config.Topology) error {
	var peers []uint64
	for _, peer := range topology.Servers {
		if peer.Id == id {
			continue
		}

		peers = append(peers, uint64(peer.Id))
	}

	dbStorage, err := storage.CreateStorage(storage.Params{})
	if err != nil {
		return fmt.Errorf("creating storage: %w", dbStorage)
	}

	raftStorage := &raft.MemoryStorage{}

	raftNode := raft.StartNode(raft.Params{
		ID:                       uint64(id),
		StateStorage:             raftStorage,
		LogStorage:               raftStorage,
		MaxLeaderElectionTimeout: 20,
		HeartBeatTimeout:         5,
		Peers:                    peers,
		Logger:                   logging.NewLogger(fmt.Sprintf("raft #%v", id), logging.DefaultLevel),
	})

	db := &node.DBNode{
		Config:            topology.Servers[id],
		Storage:           dbStorage,
		RaftNode:          raftNode,
		RaftMemoryStorage: raftStorage,
		Logger:            logging.NewLogger(fmt.Sprintf("node #%v", id), logging.DefaultLevel),
	}

	db.RaftTransport = &transport.HttpTransport{
		Raft:   db,
		Logger: logging.NewLogger(fmt.Sprintf("server #%v", id), logging.DefaultLevel),
	}
	if err = db.RaftTransport.Start(); err != nil {
		return fmt.Errorf("starting raft transport: %w", err)
	}

	for _, peer := range topology.Servers {
		if peer.Id == id {
			continue
		}

		db.RaftTransport.AddPeer(uint64(peer.Id), peer.HttpAddress())
	}

	if err := db.Init(); err != nil {
		return fmt.Errorf("initializing database: %w", err)
	}

	db.Run()

	return nil
}

func main() {
	runtime.SetBlockProfileRate(1)

	var idStr = "1"
	if len(os.Args) > 1 {
		idStr = os.Args[1]
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		panic(err)
	}

	if err := runNode(id); err != nil {
		logging.System.Errorf("Node exited with error: %v", err)
		return
	}

	<-util.WaitForTerminationRequest()
}
