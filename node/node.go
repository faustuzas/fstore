package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/faustuzas/distributed-kv/config"
	"github.com/faustuzas/distributed-kv/logging"
	"github.com/faustuzas/distributed-kv/raft"
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	raftstorage "github.com/faustuzas/distributed-kv/raft/storage"
	"github.com/faustuzas/distributed-kv/raft/transport"
	"github.com/faustuzas/distributed-kv/storage"
	"github.com/gorilla/mux"
)

var _ transport.Raft = (*DBNode)(nil)

type RaftRequest struct {
	ID uint64 `json:"id"`

	Key   string `json:"kay"`
	Value string `json:"value"`
}

type DBNode struct {
	Config config.Server

	Storage storage.Storage

	RaftNode         raft.Node
	RaftTransport    transport.Transport
	RaftStateStorage raftstorage.MutableStateStorage
	RaftLogStorage   raftstorage.MutableLogStorage

	Logger  logging.Logger
	Metrics *Metrics

	httpSrv  *http.Server
	listener net.Listener

	reqIdGen  *RequestIdGenerator
	reqWaiter RequestWaiter

	stopCh chan struct{}
}

func (n *DBNode) Init() error {
	var err error
	n.httpSrv, n.listener, err = n.createServer()
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}

	n.reqIdGen = &RequestIdGenerator{}
	n.reqWaiter = &waiter{activeRequests: map[uint64]chan interface{}{}}

	return nil
}

func (n *DBNode) Run() {
	go n.runTicker()
	go func() {
		_ = n.httpSrv.Serve(n.listener)
	}()
	go n.runRaftProcess()

	n.Logger.Infof("Database node started")
}

func (n *DBNode) Process(ctx context.Context, msg pb.Message) error {
	return n.RaftNode.Step(ctx, msg)
}

func (n *DBNode) Stop() {
	// TODO: implement
}

func (n *DBNode) createServer() (*http.Server, net.Listener, error) {
	router := mux.NewRouter()
	router.Handle("/raft", n.RaftTransport.Handler())

	n.addObservabilityEndpoints(router)
	n.addAdminEndpoints(router)
	n.addDBEndpoints(router)

	l, err := net.Listen("tcp", n.Config.HttpAddress())
	if err != nil {
		return nil, nil, fmt.Errorf("creating http listener: %w", err)
	}

	return &http.Server{Handler: router}, l, nil
}

func (n *DBNode) runTicker() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.RaftNode.Tick()
		case <-n.stopCh:
			return
		}
	}
}
