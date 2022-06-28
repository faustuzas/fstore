package raft

import (
	"testing"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestNewRaftLog(t *testing.T) {
	log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 4, Index: 2}})

	r, err := newRaftLog(log)
	require.NoError(t, err)
	require.NotNil(t, r)

	// init with 0 commit index
	require.Equal(t, uint64(0), r.commitIndex)

	// set first unstable index
	require.Equal(t, uint64(3), r.unstable.offset)

	// check that basic functions are working
	lastIndex, err := r.lastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), lastIndex)

	lastTerm, err := r.lastTerm()
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastTerm)
}

func TestRaftLogAccess(t *testing.T) {
	ttable := []struct {
		name             string
		stable, unstable []pb.Entry

		expectLastTerm  uint64
		expectLastIndex uint64
	}{
		{
			name:     "if_no_entries_exist_should_return_zeroes",
			stable:   nil,
			unstable: nil,

			expectLastTerm:  0,
			expectLastIndex: 0,
		},
		{
			name:     "if_unstable_entries_exist_should_give_priority",
			stable:   []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
			unstable: []pb.Entry{{Term: 3, Index: 3}},

			expectLastTerm:  3,
			expectLastIndex: 3,
		},
		{
			name:     "if_no_unstable_entries_fallback_to_stable",
			stable:   []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
			unstable: nil,

			expectLastTerm:  1,
			expectLastIndex: 2,
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			l := &raftLog{
				stableLog: newInMemoryStorage(tt.stable),
				unstable:  newUnstableLogFromEntries(tt.unstable...),
			}

			lastIndex, _ := l.lastIndex()
			lastTerm, _ := l.lastTerm()
			require.Equal(t, tt.expectLastIndex, lastIndex)
			require.Equal(t, tt.expectLastTerm, lastTerm)
		})
	}
}

func TestIsAllowedToVote(t *testing.T) {
	ttable := []struct {
		name      string
		log       []pb.Entry
		voteTerm  uint64
		voteIndex uint64

		allowedToVote bool
	}{
		{
			name: "vote_term_is_smaller",
			log:  []pb.Entry{{Term: 3, Index: 1}, {Term: 5, Index: 2}},

			voteTerm:  2,
			voteIndex: 4,

			allowedToVote: false,
		},
		{
			name: "vote_index_is_smaller_for_the_same_term",
			log:  []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}},

			voteTerm:  2,
			voteIndex: 2,

			allowedToVote: false,
		},
		{
			name: "vote_term_and_index_matches_local",
			log:  []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}},

			voteTerm:  2,
			voteIndex: 3,

			allowedToVote: true,
		},
		{
			name: "vote_term_and_index_are_bigger",
			log:  []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}},

			voteTerm:  4,
			voteIndex: 100,

			allowedToVote: true,
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			l := raftLog{
				unstable: newUnstableLogFromEntries(tt.log...),
			}

			isAllowedToVote, _ := l.isAllowedToVote(tt.voteTerm, tt.voteIndex)
			require.Equal(t, tt.allowedToVote, isAllowedToVote)
		})
	}
}

func TestUnstableEntries(t *testing.T) {
	t.Run("no_entries", func(t *testing.T) {
		l := &raftLog{
			unstable:  unstableLog{entries: nil},
			stableLog: newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}}),
		}

		entries := l.unstableEntries()
		require.Empty(t, entries)
	})

	t.Run("some_entries", func(t *testing.T) {
		l := &raftLog{
			unstable:  newUnstableLogFromEntries([]pb.Entry{{Term: 1, Index: 2}, {Term: 3, Index: 4}}...),
			stableLog: newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}}),
		}

		entries := l.unstableEntries()
		require.Len(t, entries, 2)
		require.Equal(t, []pb.Entry{{Term: 1, Index: 2}, {Term: 3, Index: 4}}, entries)
	})
}

func TestAppendEntries(t *testing.T) {
	t.Run("appending_empty_slice", func(t *testing.T) {
		l := &raftLog{
			unstable:  unstableLog{entries: nil},
			stableLog: newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}}),
		}

		lastIndex, _ := l.appendEntries()
		require.Equal(t, uint64(1), lastIndex)
		require.Empty(t, l.unstableEntries())
	})

	t.Run("appending_new_elements_to_unstable_log", func(t *testing.T) {
		l := &raftLog{
			unstable:  newUnstableLog(nil, 3),
			stableLog: newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}),
		}

		lastIndex, _ := l.appendEntries(pb.Entry{Term: 2, Index: 3})
		require.Equal(t, uint64(3), lastIndex)
		require.Equal(t, []pb.Entry{{Term: 2, Index: 3}}, l.unstableEntries())
	})
}

func TestEntryTerm(t *testing.T) {
	ttable := []struct {
		name     string
		stable   []pb.Entry
		unstable []pb.Entry
		index    uint64

		expectTerm uint64
	}{
		{
			name:   "zero_index_holds_zero_term",
			stable: []pb.Entry{{Term: 1, Index: 1}},
			index:  0,

			expectTerm: 0,
		},
		{
			name:     "no_stable_entries",
			unstable: []pb.Entry{{Term: 5, Index: 3}, {Term: 6, Index: 4}},
			index:    4,

			expectTerm: 6,
		},
		{
			name:   "no_unstable_entries",
			stable: []pb.Entry{{Term: 3, Index: 1}, {Term: 4, Index: 2}},
			index:  2,

			expectTerm: 4,
		},
		{
			name:     "both_entries_present_index_from_stable",
			stable:   []pb.Entry{{Term: 3, Index: 1}, {Term: 4, Index: 2}},
			unstable: []pb.Entry{{Term: 5, Index: 3}, {Term: 6, Index: 4}},
			index:    2,

			expectTerm: 4,
		},
		{
			name:     "both_entries_present_index_from_unstable",
			stable:   []pb.Entry{{Term: 3, Index: 1}, {Term: 4, Index: 2}},
			unstable: []pb.Entry{{Term: 5, Index: 3}, {Term: 6, Index: 4}},
			index:    4,

			expectTerm: 6,
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			l := &raftLog{
				unstable:  newUnstableLogFromEntries(tt.unstable...),
				stableLog: newInMemoryStorage(tt.stable),
			}

			term, _ := l.term(tt.index)
			require.Equal(t, tt.expectTerm, term)
		})
	}
}

func TestSlice(t *testing.T) {
	stableEntries := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}
	unstableEntries := []pb.Entry{{Term: 3, Index: 4}, {Term: 3, Index: 5}, {Term: 3, Index: 6}}

	ttable := []struct {
		name     string
		startIdx uint64
		endIdx   uint64

		expectEntries []pb.Entry
	}{
		{
			name:     "request_empty_slice",
			startIdx: 4,
			endIdx:   4,

			expectEntries: nil,
		},
		{
			name:     "request_first_element_from_stable",
			startIdx: 1,
			endIdx:   2,

			expectEntries: []pb.Entry{{Term: 1, Index: 1}},
		},
		{
			name:     "request_last_element_from_stable",
			startIdx: 3,
			endIdx:   4,

			expectEntries: []pb.Entry{{Term: 1, Index: 3}},
		},
		{
			name:     "request_slice_only_from_stable",
			startIdx: 1,
			endIdx:   3,

			expectEntries: []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
		},
		{
			name:     "request_first_element_from_unstable",
			startIdx: 4,
			endIdx:   5,

			expectEntries: []pb.Entry{{Term: 3, Index: 4}},
		},
		{
			name:     "request_last_element_from_unstable",
			startIdx: 6,
			endIdx:   7,

			expectEntries: []pb.Entry{{Term: 3, Index: 6}},
		},
		{
			name:     "request_slice_from_unstable",
			startIdx: 4,
			endIdx:   6,

			expectEntries: []pb.Entry{{Term: 3, Index: 4}, {Term: 3, Index: 5}},
		},
		{
			name:     "request_merged_slice_from_stable_and_unstable",
			startIdx: 2,
			endIdx:   6,

			expectEntries: []pb.Entry{{Term: 1, Index: 2}, {Term: 1, Index: 3}, {Term: 3, Index: 4}, {Term: 3, Index: 5}},
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			l := &raftLog{
				unstable:  newUnstableLogFromEntries(unstableEntries...),
				stableLog: newInMemoryStorage(stableEntries),
			}

			slice, _ := l.slice(tt.startIdx, tt.endIdx)
			require.Equal(t, tt.expectEntries, slice)
		})
	}
}

func TestEntriesFrom(t *testing.T) {
	stableEntries := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}
	unstableEntries := []pb.Entry{{Term: 3, Index: 3}, {Term: 3, Index: 4}}

	ttable := []struct {
		name    string
		fromIdx uint64

		expectEntries []pb.Entry
	}{
		{
			name:    "returns_no_entries",
			fromIdx: 5,

			expectEntries: nil,
		},

		{
			name:    "request_starting_from_stable",
			fromIdx: 2,

			expectEntries: []pb.Entry{{Term: 1, Index: 2}, {Term: 3, Index: 3}, {Term: 3, Index: 4}},
		},
		{
			name:    "request_starting_from_unstable",
			fromIdx: 4,

			expectEntries: []pb.Entry{{Term: 3, Index: 4}},
		},
		{
			name:    "returns_all_entries",
			fromIdx: 1,

			expectEntries: []pb.Entry{
				{Term: 1, Index: 1}, {Term: 1, Index: 2},
				{Term: 3, Index: 3}, {Term: 3, Index: 4},
			},
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			l := &raftLog{
				unstable:  newUnstableLogFromEntries(unstableEntries...),
				stableLog: newInMemoryStorage(stableEntries),
			}

			slice, _ := l.entriesFrom(tt.fromIdx)
			require.Equal(t, tt.expectEntries, slice)
		})
	}
}

func TestAllEntries(t *testing.T) {
	ttable := []struct {
		name            string
		stableEntries   []pb.Entry
		unstableEntries []pb.Entry

		expectEntries []pb.Entry
	}{
		{
			name: "there_are_no_entries",

			expectEntries: nil,
		},
		{
			name:          "only_stable_entries_exist",
			stableEntries: []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},

			expectEntries: []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
		},
		{
			name:            "only_unstable_entries_exist",
			unstableEntries: []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},

			expectEntries: []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
		},
		{
			name:            "merge_stable_and_unstable_entries",
			stableEntries:   []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
			unstableEntries: []pb.Entry{{Term: 2, Index: 3}, {Term: 2, Index: 4}},

			expectEntries: []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 2, Index: 3}, {Term: 2, Index: 4}},
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			offset := uint64(0)
			if len(tt.unstableEntries) > 0 {
				offset = tt.unstableEntries[0].Index
			} else if len(tt.stableEntries) > 0 {
				offset = tt.stableEntries[len(tt.stableEntries)-1].Index + 1
			}

			l := &raftLog{
				unstable:  newUnstableLog(tt.unstableEntries, offset),
				stableLog: newInMemoryStorage(tt.stableEntries),
			}

			entries, _ := l.allEntries()
			require.Equal(t, tt.expectEntries, entries)
		})
	}
}

func TestTryToAppend(t *testing.T) {
	t.Run("append_first_ever_entry", func(t *testing.T) {
		l := &raftLog{
			unstable:  newUnstableLogFromEntries(),
			stableLog: newInMemoryStorage(nil),
		}

		lastIndex, ok, _ := l.tryToAppend(0, 0, pb.Entry{Term: 1, Index: 1})
		require.True(t, ok)
		require.Equal(t, uint64(1), lastIndex)
	})

	t.Run("panic_if_conflicting_on_committed_entry", func(t *testing.T) {
		l := &raftLog{
			unstable:    newUnstableLogFromEntries(),
			stableLog:   newInMemoryStorage([]pb.Entry{{Index: 1, Term: 2}, {Index: 2, Term: 2}}),
			commitIndex: 2,
		}

		require.PanicsWithValue(t, "conflict index in already committed entries", func() {
			_, _, _ = l.tryToAppend(2, 1, pb.Entry{Term: 1, Index: 1})
		})
	})

	ttable := []struct {
		name         string
		prevLogTerm  uint64
		prevLogIndex uint64
		entries      []pb.Entry

		expectOk        bool
		expectLastIndex uint64
	}{
		{
			name:         "prev_log_entry_is_too_far_in_the_log_and_does_not_exist_locally",
			prevLogTerm:  10,
			prevLogIndex: 136,

			expectOk:        false,
			expectLastIndex: 4,
		},
		{
			name:         "prev_log_entry_does_not_match_the_entry_in_the_log",
			prevLogTerm:  3,
			prevLogIndex: 4,

			expectOk:        false,
			expectLastIndex: 4,
		},
		{
			name:         "all_appendable_entries_are_new",
			prevLogTerm:  2,
			prevLogIndex: 4,
			entries:      []pb.Entry{{Term: 3, Index: 5}, {Term: 3, Index: 6}},

			expectOk:        true,
			expectLastIndex: 6,
		},
		{
			name:         "conflicting_entries_are_removed",
			prevLogTerm:  1,
			prevLogIndex: 2,
			entries:      []pb.Entry{{Term: 4, Index: 3}},

			expectOk:        true,
			expectLastIndex: 3,
		},
		{
			name:         "duplicate_entries_are_removed",
			prevLogTerm:  1,
			prevLogIndex: 2,
			entries:      []pb.Entry{{Term: 2, Index: 3}, {Term: 2, Index: 4}},

			expectOk:        true,
			expectLastIndex: 4,
		},
	}

	for _, tt := range ttable {
		initialStable := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}
		initialUnstable := []pb.Entry{{Term: 2, Index: 3}, {Term: 2, Index: 4}}

		t.Run(tt.name, func(t *testing.T) {
			l := &raftLog{
				unstable:  newUnstableLogFromEntries(initialUnstable...),
				stableLog: newInMemoryStorage(initialStable),
			}

			lastIndex, ok, _ := l.tryToAppend(tt.prevLogTerm, tt.prevLogIndex, tt.entries...)
			require.Equal(t, tt.expectOk, ok)
			require.Equal(t, tt.expectLastIndex, lastIndex)
		})
	}
}

func TestCommittedTo(t *testing.T) {
	t.Run("panics_if_the_commit_index_is_decreasing", func(t *testing.T) {
		require.Panics(t, func() {
			l := raftLog{commitIndex: 5}
			l.committedTo(3)
		})
	})

	t.Run("sets_the_commit_index", func(t *testing.T) {
		l := raftLog{commitIndex: 5}
		l.committedTo(10)
		require.Equal(t, uint64(10), l.commitIndex)
	})
}

func TestNotAppliedEntries(t *testing.T) {
	l := &raftLog{
		stableLog:   newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}),
		unstable:    unstableLog{entries: nil, offset: 4},
		commitIndex: 3,
	}

	t.Run("appliedIndex_is_zero", func(t *testing.T) {
		copied := *l
		copied.appliedIndex = 0

		require.True(t, copied.hasNotAppliedEntries())
		entries, _ := copied.notAppliedEntries()
		require.Equal(t, []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}, entries)
	})

	t.Run("appliedIndex_is_more_than_zero", func(t *testing.T) {
		copied := *l
		copied.appliedIndex = 1

		require.True(t, copied.hasNotAppliedEntries())
		entries, _ := copied.notAppliedEntries()
		require.Equal(t, []pb.Entry{{Term: 1, Index: 2}, {Term: 1, Index: 3}}, entries)
	})

	t.Run("appliedIndex_is_equal_to_committedIndex", func(t *testing.T) {
		copied := *l
		copied.appliedIndex = 3

		require.False(t, copied.hasNotAppliedEntries())
		entries, _ := copied.notAppliedEntries()
		require.Empty(t, entries)
	})
}

func TestStableToForwarding(t *testing.T) {
	log := &raftLog{
		stableLog: nil,
		unstable:  newUnstableLogFromEntries([]pb.Entry{{Term: 2, Index: 4}, {Term: 40, Index: 5}}...),
	}

	// should just forward to unstable log
	require.Equal(t, uint64(4), log.unstable.offset)
	log.stableTo(40, 5)
	require.Equal(t, uint64(6), log.unstable.offset)
}

func TestAppliedTo(t *testing.T) {
	l := &raftLog{
		stableLog:    newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}),
		unstable:     unstableLog{entries: []pb.Entry{{Term: 1, Index: 4}}, offset: 4},
		commitIndex:  3,
		appliedIndex: 2,
	}

	t.Run("panics_when_applied_index_is_decreasing", func(t *testing.T) {
		require.PanicsWithValue(t, "applied index decreased", func() {
			l.appliedTo(1)
		})
	})

	t.Run("panics_when_applied_index_is_greater_than_commit_index", func(t *testing.T) {
		require.PanicsWithValue(t, "applied index is higher than commit index", func() {
			l.appliedTo(4)
		})
	})

	t.Run("sets_the_new_applied_index", func(t *testing.T) {
		l.appliedTo(3)
		require.Equal(t, uint64(3), l.appliedIndex)
	})
}
