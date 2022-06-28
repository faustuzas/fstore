package storage

import (
	"testing"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/stretchr/testify/require"
)

type StateStorageTestSuite struct {
	t       *testing.T
	factory func(state *pb.PersistentState) MutableStateStorage
	cleanUp func()
}

func (s StateStorageTestSuite) Run() {
	if s.cleanUp == nil {
		s.cleanUp = func() {}
	}

	s.t.Run("SetState", func(t *testing.T) {
		t.Cleanup(s.cleanUp)

		t.Run("sets_not_existent", func(t *testing.T) {
			storage := s.factory(nil)
			state := pb.PersistentState{Term: 2, VotedFor: 5}
			err := storage.SetState(state)
			require.NoError(t, err)

			savedState, ok, err := storage.State()
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, state, savedState)
		})

		t.Run("overrides_existent", func(t *testing.T) {
			storage := s.factory(&pb.PersistentState{Term: 2, VotedFor: 5})
			newState := pb.PersistentState{Term: 20, VotedFor: 50}
			err := storage.SetState(newState)
			require.NoError(t, err)

			savedState, ok, err := storage.State()
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, newState, savedState)
		})
	})

	s.t.Run("State", func(t *testing.T) {
		t.Run("returns_false_if_state_is_not_known", func(t *testing.T) {
			storage := s.factory(nil)

			_, ok, _ := storage.State()
			require.False(t, ok)
		})

		t.Run("returns_the_saved_state", func(t *testing.T) {
			state := pb.PersistentState{Term: 4, VotedFor: 1}
			storage := s.factory(&state)

			st, ok, _ := storage.State()
			require.True(t, ok)
			require.Equal(t, state, st)
		})
	})
}
