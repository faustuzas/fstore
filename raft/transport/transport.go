package transport

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/faustuzas/distributed-kv/logging"
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
)

const (
	_raftBasePath = "raft"
)

type Raft interface {
	Process(ctx context.Context, msg pb.Message) error
}

// TODO: check whether there are no concurrent calls to Send

type Transport interface {
	// Start starts the server and prepares to serve requests
	Start() error

	// Send messages to the nodes by using the To field in them.
	// All messages can be dropped in the event of some failure or overloading, therefore,
	// application is responsible for retrying them
	Send(messages ...pb.Message)

	// Handler returns a http handler which has to be integrated to the main
	// application server under the path /raft
	Handler() http.Handler

	// AddPeer adds raft peer instance with a given id which can be found on url
	AddPeer(id uint64, url string)

	// Stop starts termination process and blocks until it is done
	Stop()
}

var _ Transport = (*HttpTransport)(nil)

type HttpTransport struct {
	Raft    Raft
	Logger  logging.Logger
	Encoder Encoder
	Metrics *Metrics

	peers map[uint64]chan pb.Message
	wg    sync.WaitGroup
	done  chan struct{}
}

func (t *HttpTransport) Start() error {
	t.peers = make(map[uint64]chan pb.Message)
	t.done = make(chan struct{})

	return nil
}

func (t *HttpTransport) Send(messages ...pb.Message) {
	for _, msg := range messages {
		ch, ok := t.peers[msg.To]
		if !ok {
			t.Logger.Warnf("Message to not existing peer %v", msg.To)
			continue
		}

		ch <- msg
	}
}

func (t *HttpTransport) AddPeer(id uint64, url string) {
	ch := make(chan pb.Message, 128)
	t.peers[id] = ch

	go func() {
		t.wg.Add(1)
		defer t.wg.Done()

		t.servePeer(id, url, ch)
	}()
}

func (t *HttpTransport) servePeer(id uint64, url string, msgs <-chan pb.Message) {
	var (
		path = fmt.Sprintf("http://%s/%s", url, _raftBasePath)
		buf  bytes.Buffer
	)

	for msg := range msgs {
		if err := t.Encoder.Encode(&buf, &msg); err != nil {
			t.Logger.Errorf("Failed to marshal message %+v: %v", msg, err)
			continue
		}

		t.Metrics.RecordMessage(&msg)
		t.Metrics.RecordMessageSize(buf.Len())

		if _, err := http.Post(path, t.Encoder.ContentType(), &buf); err != nil {
			t.Logger.Errorf("Failed to post message to peer %v on %v: %v", id, path, err)
		}

		buf.Reset()
	}
}

func (t *HttpTransport) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := r.Body.Close(); err != nil {
				t.Logger.Errorf("Failed closing raft request body: %v", err)
			}
		}()

		var msg pb.Message
		if err := t.Encoder.Decode(r.Body, &msg); err != nil {
			t.Logger.Errorf("Unable to decode message: %v", err)
			return
		}

		if err := t.Raft.Process(r.Context(), msg); err != nil {
			t.Logger.Errorf("Unable to process message (%v) received from %s: %v", msg.Type, r.Host, err)
		}
	})
}

func (t *HttpTransport) Stop() {
	for _, ch := range t.peers {
		close(ch)
	}

	t.wg.Wait()
}
