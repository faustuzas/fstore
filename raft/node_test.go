package raft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/faustuzas/distributed-kv/logging"
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestNodePropose(t *testing.T) {
	t.Run("submitting_proposal_to_state_machine_respects_stopped_node", func(t *testing.T) {
		doneCh := make(chan struct{})
		n := node{proposeCh: nil, doneCh: doneCh}

		close(doneCh)

		withNoBlockCheck(t, func() {
			err := n.Propose(context.Background(), []byte("test1"))
			require.ErrorIs(t, err, ErrNodeIsStopped)
		})

	})

	t.Run("submitting_proposal_to_state_machine_respects_context_cancelling", func(t *testing.T) {
		n := node{proposeCh: nil}

		ctx, cancel := context.WithCancel(context.Background())
		resultCh := make(chan error)
		go func() {
			resultCh <- n.Propose(ctx, []byte("test1"))
		}()

		cancel()

		select {
		case err := <-resultCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			require.Fail(t, "context was not cancelled")
		}
	})

	t.Run("waiting_for_state_machine_results_respects_cancelled_context", func(t *testing.T) {
		propCh := make(chan proposeMsg)
		n := node{proposeCh: propCh}

		ctx, cancel := context.WithCancel(context.Background())
		resultCh := make(chan error)
		go func() {
			resultCh <- n.Propose(ctx, []byte("test1"))
		}()

		// emulate that state machine accepted the request
		<-propCh

		// cancel the request without responding to request results channel
		cancel()

		select {
		case err := <-resultCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(1 * time.Second):
			require.Fail(t, "context was not cancelled")
		}
	})

	t.Run("waiting_for_state_machine_results_respects_stopped_node", func(t *testing.T) {
		propCh := make(chan proposeMsg)
		doneCh := make(chan struct{})
		n := node{proposeCh: propCh, doneCh: doneCh}

		resultCh := make(chan error)
		go func() {
			resultCh <- n.Propose(context.Background(), []byte("test1"))
		}()

		// emulate that state machine accepted the request
		<-propCh

		// signal stopping the node before result received
		close(doneCh)

		select {
		case err := <-resultCh:
			require.ErrorIs(t, err, ErrNodeIsStopped)
		case <-time.After(1 * time.Second):
			require.Fail(t, "waiting for state machine result does not respect stopped node")
		}
	})

	t.Run("Propose_sends_message_to_appropriate_channel", func(t *testing.T) {
		proposeCh := make(chan proposeMsg, 1)
		n := node{proposeCh: proposeCh}

		testErr := fmt.Errorf("test error")

		go func() {
			err := n.Propose(context.Background(), []byte("test1"))
			require.ErrorIs(t, err, testErr)
		}()

		select {
		case msg := <-proposeCh:
			require.Equal(t, []byte("test1"), msg.data)
			require.NotNil(t, msg.resultCh)

			// Propose is blocking until state machine signals back the result
			withNoBlockCheck(t, func() {
				msg.resultCh <- testErr
			})
		case <-time.After(1 * time.Second):
			require.Fail(t, "Propose does not send message to an appropriate channel")
		}
	})
}

func TestNodeCampaign(t *testing.T) {
	t.Run("should_send_campaign_message", func(t *testing.T) {
		n := node{receiveCh: make(chan pb.Message, 1)}

		err := n.Campaign()
		require.NoError(t, err)

		withNoBlockCheck(t, func() {
			msg := <-n.receiveCh
			require.Equal(t, pb.Message{Type: pb.MsgCampaign}, msg)
		})
	})
}

func TestNodeStop(t *testing.T) {
	t.Run("should_close_required_channels", func(t *testing.T) {
		n := startDefaultNode()
		n.Stop()

		select {
		case _, open := <-n.stopCh:
			require.False(t, open)
		default:
			require.Fail(t, "stop channel is not closed")
		}

		select {
		case _, open := <-n.doneCh:
			require.False(t, open)
		default:
			require.Fail(t, "done channel is not closed")
		}
	})
}

func TestNodeTick(t *testing.T) {
	t.Run("sends_message_to_the_tick_channel", func(t *testing.T) {
		tickCh := make(chan struct{}, 1)
		n := node{
			tickCh: tickCh,
		}

		withNoBlockCheck(t, func() {
			n.Tick()
		})

		require.Len(t, tickCh, 1)
	})

	t.Run("sheds_ticks_if_the_channel_is_overloaded", func(t *testing.T) {
		tickCh := make(chan struct{}, 1)
		n := node{
			tickCh: tickCh,
			log:    logging.Noop,
		}

		withNoBlockCheck(t, func() {
			n.Tick()
		})

		// should shed this tick and do not block
		withNoBlockCheck(t, func() {
			n.Tick()
		})

		require.Len(t, tickCh, 1)
	})
}

func TestNodeProgress(t *testing.T) {
	t.Run("returns_original_channel", func(t *testing.T) {
		ch := make(chan Progress)
		n := node{progressCh: ch}

		var readOnly <-chan Progress = ch
		require.Equal(t, readOnly, n.Progress())
	})
}

func TestNodeAdvance(t *testing.T) {
	t.Run("sends_a_message_to_the_Advance_channel", func(t *testing.T) {
		ch := make(chan struct{}, 1)

		n := node{advanceCh: ch}
		n.Advance()

		require.Len(t, ch, 1)
	})

	t.Run("does_not_block_if_node_is_stopped", func(t *testing.T) {
		doneCh := make(chan struct{})
		close(doneCh)

		n := node{advanceCh: make(chan struct{}), doneCh: doneCh}

		resultCh := make(chan struct{})
		go func() {
			n.Advance()
			close(resultCh)
		}()

		select {
		case <-resultCh:
		case <-time.After(1 * time.Second):
			require.Fail(t, "Advance blocks")
		}
	})
}

func TestNodeStep(t *testing.T) {
	t.Run("passes_message_into_correct_channel", func(t *testing.T) {
		ch := make(chan pb.Message, 1)
		n := node{receiveCh: ch}

		withNoBlockCheck(t, func() {
			err := n.Step(context.Background(), pb.Message{})
			require.NoError(t, err)
		})

		require.Len(t, ch, 1)
	})

	t.Run("respect_context_when_blocking", func(t *testing.T) {
		n := node{receiveCh: make(chan pb.Message)}

		result := make(chan error)
		ctx, cancelFn := context.WithCancel(context.Background())

		go func() {
			result <- n.Step(ctx, pb.Message{})
		}()

		select {
		case <-result:
			require.Fail(t, "result should block")
		case <-time.After(10 * time.Millisecond):
			// expected blocking
		}

		cancelFn()

		withNoBlockCheck(t, func() {
			err := <-result
			require.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("does_not_block_if_node_is_stopped", func(t *testing.T) {
		doneCh := make(chan struct{})
		close(doneCh)

		n := node{receiveCh: make(chan pb.Message), doneCh: doneCh}

		withNoBlockCheck(t, func() {
			err := n.Step(context.Background(), pb.Message{})
			require.ErrorIs(t, err, ErrNodeIsStopped)
		})
	})
}

func TestNodeIntegration(t *testing.T) {
	t.Run("submitting_proposal_to_state_machine_respects_stopped_node", func(t *testing.T) {
		n := startDefaultNode()
		n.Stop()

		withNoBlockCheck(t, func() {
			err := n.Propose(context.Background(), []byte("test1"))
			require.ErrorIs(t, err, ErrNodeIsStopped)
		})
	})

	t.Run("leader_node_accepts_proposed_values", func(t *testing.T) {
		n := startDefaultNode()
		defer n.Stop()

		promoteToLeader(n)

		withNoBlockCheck(t, func() {
			err := n.Propose(context.Background(), []byte("test1"))
			require.NoError(t, err)
		})

		withNoBlockCheck(t, func() {
			pg := <-n.Progress()
			require.Len(t, pg.EntriesToPersist, 1)
			require.Equal(t, []byte("test1"), pg.EntriesToPersist[0].Data)
		})
	})

	t.Run("tick_moves_node_internal_clock_forward", func(t *testing.T) {
		n := startDefaultNode()
		defer n.Stop()

		for i := 0; i < _testElectionTimeout; i++ {
			withNoBlockCheck(t, func() {
				n.Tick()
			})
		}

		waitUntilEmpty(t, n.tickCh)

		withNoBlockCheck(t, func() {
			pg := <-n.Progress()

			// ticks invoked leader election
			require.Equal(t, RoleCandidate, pg.SoftState.Role)
		})
	})

	t.Run("messages_passed_to_step_are_passed_to_raft", func(t *testing.T) {
		n := startDefaultNode()
		defer n.Stop()

		withNoBlockCheck(t, func() {
			_ = n.Step(context.Background(), pb.Message{
				Type:     pb.MsgVote,
				From:     10,
				To:       _localNodeId,
				Term:     100,
				LogIndex: 0,
				LogTerm:  0,
			})
		})

		withNoBlockCheck(t, func() {
			pg := <-n.Progress()
			require.Equal(t, pb.MsgVoteRes, pg.Messages[0].Type)
		})
	})

	t.Run("next_progress_is_not_returned_until_advance_is_not_called", func(t *testing.T) {
		n := startDefaultNode()
		defer n.Stop()

		msg := pb.Message{
			Type:     pb.MsgVote,
			From:     10,
			To:       _localNodeId,
			Term:     100,
			LogIndex: 0,
			LogTerm:  0,
		}

		_ = n.Step(context.Background(), msg)

		// first call to progress should return immediately
		withNoBlockCheck(t, func() {
			pg := <-n.Progress()
			require.Equal(t, pb.MsgVoteRes, pg.Messages[0].Type)
			require.Equal(t, uint64(100), pg.Messages[0].Term)
		})

		// send another message to trigger new progress creation
		msg.Term = 102
		_ = n.Step(context.Background(), msg)

		select {
		case <-n.Progress():
			require.Fail(t, "should block until advance is called")
		case <-time.After(200 * time.Millisecond):
			// all good, channel blocks
		}

		n.Advance()

		// since advance is called should return immediately again
		withNoBlockCheck(t, func() {
			pg := <-n.Progress()
			require.Equal(t, pb.MsgVoteRes, pg.Messages[0].Type)
			require.Equal(t, uint64(102), pg.Messages[0].Term)
		})
	})

	t.Run("empty_progress_is_not_returned_and_blocks_until_some_work_is_present", func(t *testing.T) {
		n := startDefaultNode()
		defer n.Stop()

		select {
		case <-n.Progress():
			require.Fail(t, "should block since no work is done yet")
		case <-time.After(200 * time.Millisecond):
			// all good, channel blocks
		}

		// do some work
		_ = n.Step(context.Background(), pb.Message{
			Type:     pb.MsgVote,
			From:     10,
			To:       _localNodeId,
			Term:     100,
			LogIndex: 0,
			LogTerm:  0,
		})

		// progress returns without blocking
		withNoBlockCheck(t, func() {
			pg := <-n.Progress()
			require.NotEmpty(t, pg)
		})
	})

	t.Run("should_stop_run_goroutine", func(t *testing.T) {
		n := startDefaultNode()
		withNoBlockCheck(t, func() {
			n.Stop()
		})

		// goleak invoked in TestMain would panic if there would be any dangling goroutine left too,
		// so essentially here we are asserting that the run loop was closed
	})

	t.Run("stop_should_be_idempotent", func(t *testing.T) {
		n := startDefaultNode()

		// calling Stop several times does not cause any trouble
		withNoBlockCheck(t, func() {
			n.Stop()
			n.Stop()
		})
	})
}

func TestMain(m *testing.M) {
	// checks whether all goroutines are finished after tests are done
	goleak.VerifyTestMain(m)
}

func startDefaultNode() *node {
	n, err := StartNode(Params{
		ID:                       _localNodeId,
		StateStorage:             &MemoryStorage{state: &pb.PersistentState{Term: 3, VotedFor: None}},
		LogStorage:               &MemoryStorage{},
		MaxLeaderElectionTimeout: _testElectionTimeout,
		HeartBeatTimeout:         _testHeartBeatTimeout,
		Peers:                    []uint64{10, 11},
		Logger:                   logging.Noop,
	})

	if err != nil {
		panic(err)
	}

	return n.(*node)
}

func promoteToLeader(node *node) {
	node.r.r.becomeCandidate()
	node.r.r.becomeLeader()

	// unblock the run loop and let the next Progress to be collected
	node.Tick()

	// discard messages / log entries which are generated during leader election
	<-node.Progress()
	node.Advance()
}

// waitUntilEmpty waits until the channel becomes empty
func waitUntilEmpty[T any](t *testing.T, c chan T) {
	resultCh := make(chan struct{})
	go func() {
		for len(c) != 0 {
			time.Sleep(100 * time.Millisecond)
		}
		resultCh <- struct{}{}
	}()

	withNoBlockCheck(t, func() {
		<-resultCh
	})
}

// withNoBlockCheck ensures that the function f which might have blocking behaviour
// does not block for more than 1 second making tests unresponsive
func withNoBlockCheck(t *testing.T, f func()) {
	resultCh := make(chan struct{})
	go func() {
		f()
		resultCh <- struct{}{}
	}()

	select {
	case <-resultCh:
		return
	case <-time.After(1 * time.Second):
		require.Fail(t, "function blocks for more than 1 second")
		return
	}
}
