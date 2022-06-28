package storage

import (
	"testing"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestInMemoryStateStorage(t *testing.T) {
	StateStorageTestSuite{
		t: t,
		factory: func(state *pb.PersistentState) MutableStateStorage {
			return &InMemory{state: state}
		},
	}.Run()
}

func TestEntries(t *testing.T) {
	persistedEntries := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}

	t.Run("return_first_element", func(t *testing.T) {
		storage := InMemory{entries: persistedEntries}

		slice, _ := storage.Entries(1, 2)
		require.Len(t, slice, 1)
		require.Equal(t, pb.Entry{Term: 1, Index: 1}, slice[0])
	})

	t.Run("return_all_elements", func(t *testing.T) {
		storage := InMemory{entries: persistedEntries}

		slice, _ := storage.Entries(1, 4)
		require.Equal(t, []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}, slice)
	})
}

func TestTerm(t *testing.T) {
	t.Run("returns_0_when_asked_for_0_index", func(t *testing.T) {
		storage := InMemory{entries: []pb.Entry{{Term: 4, Index: 1}, {Term: 5, Index: 2}, {Term: 6, Index: 3}}}

		term, err := storage.Term(0)
		require.NoError(t, err)
		require.Zero(t, term)
	})

	t.Run("returns_requested_raft_entry_term", func(t *testing.T) {
		storage := InMemory{entries: []pb.Entry{{Term: 4, Index: 1}, {Term: 5, Index: 2}, {Term: 6, Index: 3}}}

		term, _ := storage.Term(2)
		require.Equal(t, uint64(5), term)
	})
}

func TestLastIndex(t *testing.T) {
	t.Run("no_entries_currently_saved_return_0", func(t *testing.T) {
		storage := InMemory{entries: nil}

		lastIndex, err := storage.LastIndex()
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastIndex)
	})

	t.Run("return_last_entry_index", func(t *testing.T) {
		storage := InMemory{entries: []pb.Entry{{Index: 5}, {Index: 6}}}

		lastIndex, _ := storage.LastIndex()
		require.Equal(t, uint64(6), lastIndex)
	})
}

func TestAppend(t *testing.T) {
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
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			storage := InMemory{entries: persistedEntries}

			storage.Append(tt.newEntries...)

			require.Equal(t, tt.expectEntries, storage.entries)
		})
	}

	t.Run("append_to_empty_storage", func(t *testing.T) {
		storage := InMemory{entries: nil}

		storage.Append(persistedEntries...)

		require.Equal(t, persistedEntries, storage.entries)
	})

	t.Run("append_empty_slice", func(t *testing.T) {
		storage := InMemory{entries: persistedEntries}

		storage.Append()

		require.Equal(t, persistedEntries, storage.entries)
	})

	t.Run("when_the_log_encounters_missing_index_it_panics", func(t *testing.T) {
		require.PanicsWithValue(t, "missing entries in the log", func() {
			storage := InMemory{entries: []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}}
			storage.Append(pb.Entry{Term: 2, Index: 4})
		})
	})
}
