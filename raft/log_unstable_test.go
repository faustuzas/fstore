package raft

import (
	"testing"

	pb "github.com/faustuzas/fstore/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestMaybeLastIndex(t *testing.T) {
	t.Run("empty log", func(t *testing.T) {
		var empty unstableLog

		_, ok := empty.maybeLastIndex()
		require.False(t, ok)
	})

	t.Run("log with elements", func(t *testing.T) {
		log := unstableLog{entries: []pb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 3}}}

		index, ok := log.maybeLastIndex()
		require.True(t, ok)
		require.Equal(t, uint64(5), index)
	})
}

func TestMaybeTerm(t *testing.T) {
	entries := []pb.Entry{{Index: 5, Term: 3}, {Index: 6, Term: 4}}

	ttable := []struct {
		name  string
		index uint64

		expectOk   bool
		expectTerm uint64
	}{
		{
			name:  "index is bigger than all others",
			index: 4,

			expectOk: false,
		},
		{
			name:  "index is smaller than all others",
			index: 7,

			expectOk: false,
		},
		{
			name:  "index falls into range",
			index: 6,

			expectOk:   true,
			expectTerm: 4,
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			log := newUnstableLogFromEntries(entries...)

			term, ok := log.maybeTerm(tt.index)
			require.Equal(t, tt.expectOk, ok)

			if ok {
				require.Equal(t, tt.expectTerm, term)
			}
		})
	}

	t.Run("empty log", func(t *testing.T) {
		log := unstableLog{entries: nil}

		_, ok := log.maybeTerm(3)
		require.False(t, ok)
	})
}

func TestAppendUnstableEntries(t *testing.T) {
	ttable := []struct {
		name       string
		appEntries []pb.Entry

		expectEntries []pb.Entry
		expectOffset  uint64
	}{
		{
			name:       "append no entries",
			appEntries: nil,

			expectEntries: []pb.Entry{{Term: 1, Index: 4}, {Term: 2, Index: 5}},
			expectOffset:  4,
		},
		{
			name:       "append only duplicate entries",
			appEntries: []pb.Entry{{Term: 1, Index: 4}, {Term: 2, Index: 5}},

			expectEntries: []pb.Entry{{Term: 1, Index: 4}, {Term: 2, Index: 5}},
			expectOffset:  4,
		},
		{
			name:       "append entry with conflict in the begging of the log",
			appEntries: []pb.Entry{{Term: 3, Index: 4}},

			expectEntries: []pb.Entry{{Term: 3, Index: 4}},
			expectOffset:  4,
		},
		{
			name:       "append entry with conflict in the end of the log",
			appEntries: []pb.Entry{{Term: 3, Index: 5}},

			expectEntries: []pb.Entry{{Term: 1, Index: 4}, {Term: 3, Index: 5}},
			expectOffset:  4,
		},
		{
			name:       "append some conflicting entries before the start of the current log",
			appEntries: []pb.Entry{{Term: 3, Index: 2}, {Term: 3, Index: 3}},

			expectEntries: []pb.Entry{{Term: 3, Index: 2}, {Term: 3, Index: 3}},
			expectOffset:  2,
		},
		{
			name:       "append some conflicting and some new entries",
			appEntries: []pb.Entry{{Term: 3, Index: 5}, {Term: 3, Index: 6}},

			expectEntries: []pb.Entry{{Term: 1, Index: 4}, {Term: 3, Index: 5}, {Term: 3, Index: 6}},
			expectOffset:  4,
		},
		{
			name:       "append only new entries",
			appEntries: []pb.Entry{{Term: 5, Index: 6}, {Term: 5, Index: 7}},

			expectEntries: []pb.Entry{{Term: 1, Index: 4}, {Term: 2, Index: 5}, {Term: 5, Index: 6}, {Term: 5, Index: 7}},
			expectOffset:  4,
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			initialEntries := []pb.Entry{{Term: 1, Index: 4}, {Term: 2, Index: 5}}

			log := newUnstableLogFromEntries(initialEntries...)

			log.appendEntries(tt.appEntries...)

			require.Equal(t, tt.expectEntries, log.entries)
			require.Equal(t, tt.expectOffset, log.offset)
		})
	}
}

func TestUnstableSlice(t *testing.T) {
	entries := []pb.Entry{{Term: 1, Index: 5}, {Term: 4, Index: 6}, {Term: 4, Index: 7}}

	ttable := []struct {
		name     string
		startIdx uint64
		endIdx   uint64

		expectEntries []pb.Entry
	}{
		{
			name:     "request empty slice",
			startIdx: 5,
			endIdx:   5,

			expectEntries: []pb.Entry{},
		},
		{
			name:     "request first element",
			startIdx: 5,
			endIdx:   6,

			expectEntries: []pb.Entry{{Term: 1, Index: 5}},
		},
		{
			name:     "request last element",
			startIdx: 7,
			endIdx:   8,

			expectEntries: []pb.Entry{{Term: 4, Index: 7}},
		},
		{
			name:     "request all elements",
			startIdx: 5,
			endIdx:   8,

			expectEntries: entries,
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			log := newUnstableLogFromEntries(entries...)

			slice := log.slice(tt.startIdx, tt.endIdx)
			require.Equal(t, tt.expectEntries, slice)
		})
	}
}

func TestStableTo(t *testing.T) {
	ttable := []struct {
		name    string
		entries []pb.Entry

		index uint64
		term  uint64

		expectOffset  uint64
		expectEntries []pb.Entry
	}{
		{
			name:    "completely empty",
			entries: []pb.Entry{},

			index: 5,
			term:  3,

			expectOffset:  1,
			expectEntries: []pb.Entry{},
		},
		{
			name:    "stable to already stable index",
			entries: []pb.Entry{{Index: 6, Term: 5}},

			index: 2,
			term:  1,

			expectOffset:  6,
			expectEntries: []pb.Entry{{Index: 6, Term: 5}},
		},
		{
			name:    "terms do not match",
			entries: []pb.Entry{{Index: 6, Term: 5}},

			index: 6,
			term:  4,

			expectOffset:  6,
			expectEntries: []pb.Entry{{Index: 6, Term: 5}},
		},
		{
			name:    "some of the entries are discarded",
			entries: []pb.Entry{{Index: 6, Term: 5}, {Index: 7, Term: 6}, {Index: 8, Term: 7}},

			index: 6,
			term:  5,

			expectOffset:  7,
			expectEntries: []pb.Entry{{Index: 7, Term: 6}, {Index: 8, Term: 7}},
		},
		{
			name:    "all entries are discarded",
			entries: []pb.Entry{{Index: 6, Term: 5}, {Index: 7, Term: 6}, {Index: 8, Term: 7}},

			index: 8,
			term:  7,

			expectOffset:  9,
			expectEntries: []pb.Entry{},
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			log := newUnstableLogFromEntries(tt.entries...)

			log.stableTo(tt.term, tt.index)

			require.Equal(t, tt.expectOffset, log.offset)
			require.Equal(t, tt.expectEntries, log.entries)
		})
	}
}

func newUnstableLogFromEntries(entries ...pb.Entry) unstableLog {
	offset := uint64(1)
	if len(entries) > 0 {
		offset = entries[0].Index
	}

	return newUnstableLog(entries, offset)
}

func newUnstableLog(entries []pb.Entry, offset uint64) unstableLog {
	return unstableLog{
		entries: entries,
		offset:  offset,
	}
}
