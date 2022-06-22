package node

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/faustuzas/distributed-kv/config"
	"github.com/faustuzas/distributed-kv/logging"
	"github.com/faustuzas/distributed-kv/raft"
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/faustuzas/distributed-kv/raft/transport"
	"github.com/faustuzas/distributed-kv/storage"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net"
	"net/http"
	"time"
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

	RaftNode          raft.Node
	RaftTransport     transport.Transport
	RaftMemoryStorage *raft.MemoryStorage

	Logger logging.Logger

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

}

func (n *DBNode) dbGetKey(ctx context.Context, key string) (string, error) {
	return n.Storage.Get(ctx, key)
}

func (n *DBNode) dbSetKey(ctx context.Context, key, value string) error {
	req := RaftRequest{
		ID:    n.reqIdGen.Next(),
		Key:   key,
		Value: value,
	}

	bytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("serialising request: %w", err)
	}

	ch := n.reqWaiter.Register(req.ID)

	if err = n.RaftNode.Propose(ctx, bytes); err != nil {
		return fmt.Errorf("proposing value: %w", err)
	}

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for request to finish: %w", ctx.Err())
	}
}

func (n *DBNode) runRaftProcess() {
	var prevHardState pb.PersistentState

	for {
		select {
		case progress := <-n.RaftNode.Progress():
			if !raft.ArePersistentStatesEqual(prevHardState, progress.HardState) {
				if err := n.RaftMemoryStorage.SetState(progress.HardState); err != nil {
					panic(err)
				}
				prevHardState = progress.HardState
			}

			if err := n.RaftMemoryStorage.Append(progress.EntriesToPersist...); err != nil {
				panic(err)
			}

			n.RaftTransport.Send(progress.Messages...)

			n.RaftNode.Advance()

			for _, entry := range progress.EntriesToApply {
				var req RaftRequest
				_ = json.Unmarshal(entry.Data, &req)

				_ = n.Storage.Set(context.Background(), req.Key, req.Value)
				n.reqWaiter.Trigger(req.ID, req)
			}
		}
	}
}

func (n *DBNode) createServer() (*http.Server, net.Listener, error) {
	router := mux.NewRouter()
	router.Handle("/raft", n.RaftTransport.Handler())

	attachProfiler(router)

	router.HandleFunc("/admin/snapshot", func(w http.ResponseWriter, r *http.Request) {
		snapshot, _ := n.Storage.Snapshot(context.Background())
		_, _ = w.Write([]byte(snapshot))
	}).Methods("GET")

	router.HandleFunc("/v1/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]

		val, err := n.dbGetKey(r.Context(), key)
		if err == storage.ErrNotFound {
			w.WriteHeader(404)
			return
		} else if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		_, _ = w.Write([]byte(val))
	}).Methods("GET")

	router.HandleFunc("/v1/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		val, _ := ioutil.ReadAll(r.Body)

		n.Logger.Debugf("User request: SET %v -> %v", key, val)
		if err := n.dbSetKey(r.Context(), key, string(val)); err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		_, _ = w.Write([]byte("ok"))
	}).Methods("POST")

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
