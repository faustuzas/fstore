package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/faustuzas/distributed-kv/storage"
	"github.com/gorilla/mux"
)

func (n *DBNode) addDBEndpoints(router *mux.Router) {
	router.HandleFunc("/v1/snapshot", func(w http.ResponseWriter, r *http.Request) {
		n.Metrics.ObserveSnapshotRequest(func() {
			snapshot, _ := n.Storage.Snapshot(context.Background())
			_, _ = w.Write([]byte(snapshot))
		})
	}).Methods("GET")

	router.HandleFunc("/v1/{key}", func(w http.ResponseWriter, r *http.Request) {
		n.Metrics.ObserveGetValueRequest(func() {
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
		})
	}).Methods("GET")

	router.HandleFunc("/v1/{key}", func(w http.ResponseWriter, r *http.Request) {
		n.Metrics.ObserveSetValueRequest(func() {
			key := mux.Vars(r)["key"]
			val, _ := ioutil.ReadAll(r.Body)

			n.Logger.Debugf("User request: SET %v -> %v", key, val)
			if err := n.dbSetKey(r.Context(), key, string(val)); err != nil {
				w.WriteHeader(500)
				_, _ = w.Write([]byte(err.Error()))
				return
			}

			_, _ = w.Write([]byte("ok"))
		})
	}).Methods("POST")
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
