package raft

import (
	"context"
	"fmt"
	"github.com/faustuzas/distributed-kv/logging"
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
)

var (
	// ErrNodeIsStopped is returned when a proposal is made to a stopped node
	ErrNodeIsStopped = fmt.Errorf("raft node is stopped")
)

// Node is the application facing, thread-safe Raft node interface
type Node interface {

	// Progress returns a channel with the Progress made from the last consumed Progress message.
	// Application has to process each Progress serially and according to all the rules.
	Progress() <-chan Progress

	// Advance signals to Node that the last Progress has been processed by the application
	// and the raft can move forward
	Advance()

	// Propose a new value to be appended to the log. The method returns when the raft instance
	// accepts the proposal, however, it does not mean that it is already committed.
	// Application has to tract next Progress returns to verify that the entry is committed by the cluster.
	Propose(ctx context.Context, data []byte) error

	// Step accepts the message from other cluster nodes and passes it into the raft state machine
	Step(ctx context.Context, message pb.Message) error

	// Tick moves the internal raft node's logical clock forward
	Tick()

	// Campaign starts a premature campaign for this node
	Campaign() error

	// Stop signals to the raft node that it has to stopCh and blocks until it has doneCh so
	Stop()
}

// StartNode constructs the raft Node from the Params, starts it and
// returns it for further usage
func StartNode(params Params) (Node, error) {
	rn, err := newRawNode(params)
	if err != nil {
		return nil, fmt.Errorf("creating raw raft node: %w", err)
	}

	n := &node{
		r: rn,

		proposeCh:  make(chan proposeMsg),
		progressCh: make(chan Progress),
		advanceCh:  make(chan struct{}),
		receiveCh:  make(chan pb.Message),
		tickCh:     make(chan struct{}, 128),

		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),

		log: params.Logger,
	}

	// run the event processing loop
	go n.run()

	return n, nil
}

// proposeMsg is a helper struct which allows waiting until
// the message is accepted by the raft state machine
type proposeMsg struct {
	data     []byte
	resultCh chan error
}

// node is channel based raft Node implementation
type node struct {
	// r is a non-thead-safe implementation of raft algorithm
	r rawNode

	// channels for message processing serialization
	proposeCh  chan proposeMsg
	progressCh chan Progress
	advanceCh  chan struct{}
	receiveCh  chan pb.Message
	tickCh     chan struct{}

	// stopCh channels signals the run method that it should stopCh the loop
	stopCh chan struct{}

	// doneCh channel is used to indicate the raft node is stopped. It should be closed from the run method
	doneCh chan struct{}

	log logging.Logger
}

func (n *node) Progress() <-chan Progress {
	return n.progressCh
}

func (n *node) Advance() {
	select {
	case n.advanceCh <- struct{}{}:
	case <-n.doneCh:
	}
}

func (n *node) Propose(ctx context.Context, data []byte) error {
	resultCh := make(chan error)

	// wait until the message is accepted by the state machine
	select {
	case n.proposeCh <- proposeMsg{resultCh: resultCh, data: data}:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.doneCh:
		return ErrNodeIsStopped
	}

	// wait until the node signals back the response
	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.doneCh:
		return ErrNodeIsStopped
	}
}

func (n *node) Step(ctx context.Context, message pb.Message) error {
	select {
	case n.receiveCh <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.doneCh:
		return ErrNodeIsStopped
	}
}

func (n *node) Tick() {
	select {
	case n.tickCh <- struct{}{}:
	default:
		n.log.Warnf("Ticks are shed because they are not consumed. Node is probably overloaded...")
	}
}

func (n *node) Campaign() error {
	return n.Step(context.Background(), pb.Message{Type: pb.MsgCampaign})
}

func (n *node) Stop() {
	select {
	case <-n.doneCh:
		// already closed
		return
	default:
		// if doneCh is not yet closed, we have to signal looping "run" goroutine to stop
		// by closing stopCh and wait until it closes doneCh
		close(n.stopCh)
		<-n.doneCh
	}
}

// run is the raft processing loop which serialises all data mutations and message processing
// using channels
func (n *node) run() {
	//defer func() {
	//	if err := recover(); err != nil {
	//		debugRecover(n.r.r, err.(error))
	//	}
	//}()

	var (
		// the instance of Progress that application should process or is processing at the moment
		progress Progress

		progressCh chan Progress
		advanceCh  chan struct{}
	)

	for {
		// progressCh and advanceCh channels form a little state machine.
		// Once Progress is ready, we set the progressCh variable to an active channel and initialize a Progress instance
		// which we will try to send to the application. When the application accepts the Progress, we are setting
		// the progressCh to a nil, so no more instances of Progress would be sent. From this point forward,
		// we are waiting until the application acknowledges that the Progress has been processed on the advanceCh.
		// When the Progress is acknowledged, next Progress can be collected and the state machine can start from the
		// beginning.
		if advanceCh != nil {
			progressCh = nil
		} else if n.r.hasProgressReady() {
			var err error
			progress, err = n.r.collectProgress()
			if err != nil {
				panic("collecting progress: " + err.Error())
			}

			progressCh = n.progressCh
		}

		select {
		case prop := <-n.proposeCh:
			prop.resultCh <- n.r.propose(prop.data)
		case progressCh <- progress:
			n.r.ackProgress(progress)
			advanceCh = n.advanceCh
		case <-advanceCh:
			n.r.advance(progress)

			// reset advanceCh and progress to empty until new progress is ready to be processed
			advanceCh = nil
			progress = Progress{}
		case <-n.tickCh:
			if err := n.r.step(pb.Message{Type: pb.MsgTick}); err != nil {
				// TODO: change to error logging later
				panic("tick returned an err: " + err.Error())
			}
		case msg := <-n.receiveCh:
			if err := n.r.step(msg); err != nil {
				// TODO: change to error logging later
				panic("processing receive message returned an err: " + err.Error())
			}
		case <-n.stopCh:
			close(n.doneCh)
			return
		}
	}
}
