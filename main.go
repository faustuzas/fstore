package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/faustuzas/distributed-kv/config"
	"github.com/faustuzas/distributed-kv/logging"
	"github.com/faustuzas/distributed-kv/node"
	"github.com/faustuzas/distributed-kv/raft"
	raftstorage "github.com/faustuzas/distributed-kv/raft/storage"
	"github.com/faustuzas/distributed-kv/raft/transport"
	"github.com/faustuzas/distributed-kv/storage"
	"github.com/faustuzas/distributed-kv/util"
	"github.com/prometheus/client_golang/prometheus"
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
		return fmt.Errorf("creating storage: %w", err)
	}

	raftStorage, err := raftstorage.NewOnDiskStorage(raftstorage.OnDiskParams{
		DataDir:      topology.Servers[id].DataDir,
		Encoder:      raftstorage.NewJsonEncoder(),
		Metrics:      raftstorage.NewMetrics(prometheus.DefaultRegisterer),
		MaxBlockSize: 1024 * 1024, // TODO: looks like it does not work, created files are way smaller
	})
	if err != nil {
		return fmt.Errorf("creating raft storage: %w", err)
	}

	raftNode, err := raft.StartNode(raft.Params{
		ID:                       uint64(id),
		StateStorage:             raftStorage,
		LogStorage:               raftStorage,
		MaxLeaderElectionTimeout: 20,
		HeartBeatTimeout:         5,
		Peers:                    peers,
		Logger:                   logging.NewLogger(fmt.Sprintf("raft #%v", id), logging.DefaultLevel),
	})
	if err != nil {
		return fmt.Errorf("starting raft node: %w", err)
	}

	db := &node.DBNode{
		Config:           topology.Servers[id],
		Storage:          dbStorage,
		RaftNode:         raftNode,
		RaftStateStorage: raftStorage,
		RaftLogStorage:   raftStorage,
		Logger:           logging.NewLogger(fmt.Sprintf("node #%v", id), logging.DefaultLevel),
		Metrics:          node.NewMetrics(prometheus.DefaultRegisterer),
	}

	db.RaftTransport = &transport.HttpTransport{
		Raft:    db,
		Logger:  logging.NewLogger(fmt.Sprintf("server #%v", id), logging.DefaultLevel),
		Encoder: transport.NewProtobufEncoder(),
		Metrics: transport.NewMetrics(prometheus.DefaultRegisterer),
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

// curl -X POST http://localhost:8001/raft/admin/campaign
func main() {
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
