package raft

import (
	"testing"

	"github.com/faustuzas/distributed-kv/logging"
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestHasProgressReady(t *testing.T) {
	t.Run("prev_persistent_state_was_empty", func(t *testing.T) {
		rn := newEmptyRawNode()
		rn.prevPersistentState = pb.PersistentState{}

		rn.r.term = 5
		rn.r.votedFor = 10

		require.True(t, rn.hasProgressReady())
	})

	t.Run("persistent_state_changed", func(t *testing.T) {
		rn := newEmptyRawNode()
		rn.prevPersistentState = pb.PersistentState{
			Term:     5,
			VotedFor: 10,
		}

		rn.r.term = 6
		rn.r.votedFor = 11

		require.True(t, rn.hasProgressReady())
	})

	t.Run("there_are_unsent_messages", func(t *testing.T) {
		rn := newEmptyRawNode()
		rn.r.messages = []pb.Message{{Type: pb.MsgVote}}

		require.True(t, rn.hasProgressReady())
	})

	t.Run("there_are_unstable_log_entries", func(t *testing.T) {
		rn := newEmptyRawNode()
		rn.r.raftLog = &raftLog{unstable: newUnstableLogFromEntries(pb.Entry{Term: 1, Index: 1})}

		require.True(t, rn.hasProgressReady())
	})

	t.Run("there_are_not_applied_entries", func(t *testing.T) {
		rn := newEmptyRawNode()
		rn.r.raftLog = &raftLog{
			appliedIndex: 0,
			commitIndex:  1,
			stableLog:    newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}}),
			unstable:     newUnstableLogFromEntries(),
		}

		require.True(t, rn.hasProgressReady())
	})

	t.Run("soft_state_changed", func(t *testing.T) {
		rn := newEmptyRawNode()
		rn.r.raftLog = &raftLog{unstable: newUnstableLogFromEntries()}

		rn.prevSoftState = SoftState{
			Lead: 1,
			Role: RoleCandidate,
		}

		rn.r.leaderId = 6
		rn.r.role = RoleFollower

		require.True(t, rn.hasProgressReady())
	})

	t.Run("no_activity_in_the_node", func(t *testing.T) {
		rn := newEmptyRawNode()
		rn.prevSoftState = SoftState{
			Lead: 11,
			Role: RoleFollower,
		}
		rn.prevPersistentState = pb.PersistentState{
			Term:     5,
			VotedFor: 10,
		}

		rn.r.role = RoleFollower
		rn.r.leaderId = 11
		rn.r.term = 5
		rn.r.votedFor = 10
		rn.r.messages = nil
		rn.r.raftLog = &raftLog{
			appliedIndex: 1,
			commitIndex:  1,
			stableLog:    newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}}),
			unstable:     newUnstableLogFromEntries(),
		}

		require.False(t, rn.hasProgressReady())
	})
}

func TestCollectProgress(t *testing.T) {
	rn := newEmptyRawNode()
	rn.prevPersistentState = pb.PersistentState{}

	rn.r.role = RoleFollower
	rn.r.term = 5
	rn.r.leaderId = 41
	rn.r.votedFor = 10
	rn.r.messages = []pb.Message{{Type: pb.MsgVote}}
	rn.r.raftLog = &raftLog{
		appliedIndex: 1,
		commitIndex:  2,
		stableLog:    newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}),
		unstable:     newUnstableLogFromEntries(pb.Entry{Term: 2, Index: 3}),
	}

	progress, err := rn.collectProgress()
	require.NoError(t, err)
	require.Equal(t, SoftState{Lead: 41, Role: RoleFollower}, progress.SoftState)
	require.Equal(t, pb.PersistentState{Term: 5, VotedFor: 10}, progress.HardState)
	require.Equal(t, []pb.Message{{Type: pb.MsgVote}}, progress.Messages)
	require.Equal(t, []pb.Entry{{Term: 2, Index: 3}}, progress.EntriesToPersist)
	require.Equal(t, []pb.Entry{{Term: 1, Index: 2}}, progress.EntriesToApply)
}

func TestAckProgress(t *testing.T) {
	rn := newEmptyRawNode()
	rn.prevPersistentState = pb.PersistentState{}
	rn.prevSoftState = SoftState{}

	rn.r.messages = []pb.Message{{Type: pb.MsgVote}}

	rn.ackProgress(Progress{
		HardState: pb.PersistentState{Term: 5, VotedFor: 10},
		SoftState: SoftState{Lead: 11, Role: RoleFollower},
		Messages:  []pb.Message{{Type: pb.MsgVote}},
	})

	require.Equal(t, pb.PersistentState{Term: 5, VotedFor: 10}, rn.prevPersistentState)
	require.Equal(t, SoftState{Lead: 11, Role: RoleFollower}, rn.prevSoftState)
	require.Empty(t, rn.r.messages)
}

func TestAdvance(t *testing.T) {
	rn := newEmptyRawNode()
	rn.r.raftLog = &raftLog{
		stableLog:    newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}),
		unstable:     newUnstableLogFromEntries([]pb.Entry{{Term: 1, Index: 3}, {Term: 1, Index: 4}}...),
		commitIndex:  3,
		appliedIndex: 0,
	}

	rn.advance(Progress{
		EntriesToPersist: []pb.Entry{{Term: 1, Index: 3}},
		EntriesToApply:   []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
	})

	require.Equal(t, uint64(4), rn.r.raftLog.unstable.offset)
	require.Equal(t, uint64(2), rn.r.raftLog.appliedIndex)
}

func TestArePersistentStatesEqual(t *testing.T) {
	ttable := []struct {
		name         string
		a, b         pb.PersistentState
		expectEquals bool
	}{
		{
			name: "terms_are_different",
			a:    pb.PersistentState{Term: 4, VotedFor: 1},
			b:    pb.PersistentState{Term: 5, VotedFor: 1},

			expectEquals: false,
		},
		{
			name: "voted_for_are_different",
			a:    pb.PersistentState{Term: 5, VotedFor: 1},
			b:    pb.PersistentState{Term: 5, VotedFor: 2},

			expectEquals: false,
		},
		{
			name: "everything_is_different",
			a:    pb.PersistentState{Term: 4, VotedFor: 1},
			b:    pb.PersistentState{Term: 5, VotedFor: 6},

			expectEquals: false,
		},
		{
			name: "states_are_equal",
			a:    pb.PersistentState{Term: 4, VotedFor: 1},
			b:    pb.PersistentState{Term: 4, VotedFor: 1},

			expectEquals: true,
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expectEquals, ArePersistentStatesEqual(tt.a, tt.b))
		})
	}
}

func TestAreSoftStatesEqual(t *testing.T) {
	ttable := []struct {
		name         string
		a, b         SoftState
		expectEquals bool
	}{
		{
			name: "leads_are_different",
			a:    SoftState{Lead: 4, Role: RoleCandidate},
			b:    SoftState{Lead: 5, Role: RoleCandidate},

			expectEquals: false,
		},
		{
			name: "roles_are_different",
			a:    SoftState{Lead: 5, Role: RoleFollower},
			b:    SoftState{Lead: 5, Role: RoleCandidate},

			expectEquals: false,
		},
		{
			name: "everything_is_different",
			a:    SoftState{Lead: 4, Role: RoleCandidate},
			b:    SoftState{Lead: 5, Role: RoleLeader},

			expectEquals: false,
		},
		{
			name: "states_are_equal",
			a:    SoftState{Lead: 4, Role: RoleCandidate},
			b:    SoftState{Lead: 4, Role: RoleCandidate},

			expectEquals: true,
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expectEquals, AreSoftStatesEqual(tt.a, tt.b))
		})
	}
}

func TestNewRawNode(t *testing.T) {
	params := Params{
		ID:                       5,
		StateStorage:             newInMemoryStorageWithState(&pb.PersistentState{Term: 4, VotedFor: 11}),
		LogStorage:               newInMemoryStorage(nil),
		MaxLeaderElectionTimeout: 10,
		HeartBeatTimeout:         3,
		Peers:                    nil,
		Logger:                   nil,
	}

	t.Run("initializes_raft", func(t *testing.T) {
		rn, err := newRawNode(params)

		require.NoError(t, err)
		require.Equal(t, uint64(5), rn.r.id)
		require.Equal(t, 10, rn.r.maxLeaderElectionTimeout)
		require.Equal(t, 3, rn.r.heartBeatTimeout)
	})

	t.Run("saves_persistent_state", func(t *testing.T) {
		rn, _ := newRawNode(params)

		require.Equal(t, pb.PersistentState{Term: 4, VotedFor: 11}, rn.prevPersistentState)
	})

	t.Run("saves_default_soft_state", func(t *testing.T) {
		rn, _ := newRawNode(params)

		require.Equal(t, SoftState{Lead: None, Role: RoleFollower}, rn.prevSoftState)
	})
}

func TestPropose(t *testing.T) {
	params := Params{
		ID:                       5,
		StateStorage:             newInMemoryStorageWithState(&pb.PersistentState{Term: 5, VotedFor: None}),
		LogStorage:               newInMemoryStorage(nil),
		MaxLeaderElectionTimeout: 5,
		HeartBeatTimeout:         3,
		Peers:                    nil,
		Logger:                   logging.NewLogger("test", logging.Info),
	}

	t.Run("should_append_to_the_log_if_leader", func(t *testing.T) {
		rn, _ := newRawNode(params)
		_ = rn.r.becomeCandidate()
		_ = rn.r.becomeLeader()

		err := rn.propose([]byte("hello there!"))
		require.NoError(t, err)

		progress, _ := rn.collectProgress()
		require.Equal(t, []pb.Entry{{Index: 1, Term: 6}, {Index: 2, Term: 6, Data: []byte("hello there!")}}, progress.EntriesToPersist)
	})

	t.Run("should_return_an_error_if_not_leader", func(t *testing.T) {
		rn, _ := newRawNode(params)

		err := rn.propose([]byte("hello there!"))
		require.ErrorIs(t, err, ErrNotLeader)
	})
}

func TestStep(t *testing.T) {
	params := Params{
		ID:                       5,
		StateStorage:             newInMemoryStorageWithState(&pb.PersistentState{Term: 5, VotedFor: None}),
		LogStorage:               newInMemoryStorage(nil),
		MaxLeaderElectionTimeout: 5,
		HeartBeatTimeout:         3,
		Peers:                    nil,
		Logger:                   logging.NewLogger("test", logging.Info),
	}

	t.Run("should_forward_messages_to_internal_raft_implementation", func(t *testing.T) {
		rn, _ := newRawNode(params)

		require.Equal(t, 0, rn.r.ticksFromLeaderAction)

		err := rn.step(pb.Message{Type: pb.MsgTick})
		require.NoError(t, err)
		require.Equal(t, 1, rn.r.ticksFromLeaderAction)
	})
}

func newEmptyRawNode() rawNode {
	return rawNode{r: &raft{}}
}
