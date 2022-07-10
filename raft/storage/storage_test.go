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

func (s *StateStorageTestSuite) Run() {
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

type LogStorageTestSuite struct {
	t          *testing.T
	factory    func(entries []pb.Entry) MutableLogStorage
	allEntries func(storage MutableLogStorage) []pb.Entry
	cleanUp    func()
}

func (s *LogStorageTestSuite) Run() {
	if s.cleanUp == nil {
		s.cleanUp = func() {}
	}

	s.t.Run("Entries", func(t *testing.T) {
		t.Cleanup(s.cleanUp)

		persistedEntries := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}

		t.Run("return_first_element", func(t *testing.T) {
			storage := s.factory(persistedEntries)

			slice, _ := storage.Entries(1, 2)
			require.Len(t, slice, 1)
			require.Equal(t, pb.Entry{Term: 1, Index: 1}, slice[0])
		})

		t.Run("return_all_elements", func(t *testing.T) {
			storage := s.factory(persistedEntries)

			slice, _ := storage.Entries(1, 4)
			require.Equal(t, []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}, slice)
		})
	})

	s.t.Run("Term", func(t *testing.T) {
		t.Run("returns_0_when_asked_for_0_index", func(t *testing.T) {
			storage := s.factory([]pb.Entry{{Term: 4, Index: 1}, {Term: 5, Index: 2}, {Term: 6, Index: 3}})

			term, err := storage.Term(0)
			require.NoError(t, err)
			require.Zero(t, term)
		})

		t.Run("returns_requested_raft_entry_term", func(t *testing.T) {
			storage := s.factory([]pb.Entry{{Term: 4, Index: 1}, {Term: 5, Index: 2}, {Term: 6, Index: 3}})

			term, _ := storage.Term(2)
			require.Equal(t, uint64(5), term)
		})
	})

	s.t.Run("LastIndex", func(t *testing.T) {
		t.Run("no_entries_currently_saved_return_0", func(t *testing.T) {
			storage := s.factory(nil)

			lastIndex, err := storage.LastIndex()
			require.NoError(t, err)
			require.Equal(t, uint64(0), lastIndex)
		})

		t.Run("return_last_entry_index", func(t *testing.T) {
			storage := s.factory([]pb.Entry{{Index: 5}, {Index: 6}})

			lastIndex, _ := storage.LastIndex()
			require.Equal(t, uint64(6), lastIndex)
		})
	})

	s.t.Run("Append", func(t *testing.T) {
		persistedEntries := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}

		ttable := []struct {
			name string

			// entries that will be appended
			newEntries []pb.Entry

			// entries that should exist after the operation
			expectEntries []pb.Entry
		}{
			{
				"simple_append",
				[]pb.Entry{{Term: 1, Index: 4}},
				[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}, {Term: 1, Index: 4}},
			},
			{
				"append_already_appended_entry",
				[]pb.Entry{{Term: 1, Index: 3}},
				[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}},
			},
			{
				"append_already_appended_multiple_entries",
				[]pb.Entry{{Term: 1, Index: 2}, {Term: 1, Index: 3}},
				[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}},
			},
			{
				"append_some_already_appended_entries_and_some_new_ones",
				[]pb.Entry{{Term: 1, Index: 3}, {Term: 1, Index: 4}},
				[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}, {Term: 1, Index: 4}},
			},
			{
				"override_some_uncommitted_entries_and_append_new_ones",
				[]pb.Entry{{Term: 2, Index: 3}, {Term: 2, Index: 4}},
				[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 2, Index: 3}, {Term: 2, Index: 4}},
			},
			{
				"override_entries_from_the_start",
				[]pb.Entry{{Term: 14, Index: 1}, {Term: 14, Index: 2}},
				[]pb.Entry{{Term: 14, Index: 1}, {Term: 14, Index: 2}},
			},
		}

		for _, tt := range ttable {
			t.Run(tt.name, func(t *testing.T) {
				storage := s.factory(persistedEntries)

				err := storage.Append(tt.newEntries...)
				require.NoError(t, err)
				require.Equal(t, tt.expectEntries, s.allEntries(storage))
			})
		}

		t.Run("append_to_empty_storage", func(t *testing.T) {
			storage := s.factory(nil)

			err := storage.Append(persistedEntries...)
			require.NoError(t, err)
			require.Equal(t, persistedEntries, s.allEntries(storage))
		})

		t.Run("append_empty_slice", func(t *testing.T) {
			storage := s.factory(persistedEntries)

			err := storage.Append()
			require.NoError(t, err)
			require.Equal(t, persistedEntries, s.allEntries(storage))
		})

		t.Run("when_the_log_encounters_missing_index_it_panics", func(t *testing.T) {
			require.PanicsWithValue(t, "missing entries in the log", func() {
				storage := InMemory{entries: []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}}
				_ = storage.Append(pb.Entry{Term: 2, Index: 4})
			})
		})
	})
}
