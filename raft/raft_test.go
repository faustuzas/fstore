package raft

import (
	"fmt"
	"github.com/faustuzas/distributed-kv/raft/storage"
	"testing"

	"github.com/faustuzas/distributed-kv/logging"
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/stretchr/testify/require"
)

const (
	_localNodeId          = uint64(1)
	_testElectionTimeout  = 5
	_testHeartBeatTimeout = 2
)

func TestSingleNodeStep(t *testing.T) {
	t.Run("candidate_must_proceed_only_from_follower_or_candidate_state", func(t *testing.T) {
		r := raft{role: RoleLeader}

		require.PanicsWithValue(t, "becoming a candidate from leader state", func() {
			_ = r.becomeCandidate()
		})
	})

	t.Run("after_becoming_a_candidate_node_sets_new_bounded_and_randomized_leader_election_timeout", func(t *testing.T) {
		r := createRaft(nil)
		r.maxLeaderElectionTimeout = 10

		// set to nil, so rand.Intn would be used when generating next leader election timeout
		r.randIntn = nil

		// verify that the sum of all generated timeouts
		// is in bounds [maxLeaderElectionTimeout, 2*maxLeaderElectionTimeout] * 10
		timeoutsSum := 0
		for i := 0; i < 10; i++ {
			_ = r.becomeCandidate()
			timeoutsSum += r.leaderElectionTimeout
		}
		require.LessOrEqual(t, 50, timeoutsSum)
		require.GreaterOrEqual(t, 100, timeoutsSum)
	})

	t.Run("leader_must_proceed_only_from_candidate_state", func(t *testing.T) {
		r := raft{role: RoleFollower}

		require.PanicsWithValue(t, "becoming a leader from follower state", func() {
			_ = r.becomeLeader()
		})
	})

	t.Run("leader_ignores_campaign_messages", func(t *testing.T) {
		r := createRaft(nil)
		r.term = 2
		r.peers = []uint64{10, 11}
		r.role = RoleCandidate

		_ = r.becomeLeader()
		_ = drainMessages(r)

		_ = r.step(pb.Message{Type: pb.MsgCampaign})

		require.Equal(t, RoleLeader, r.role)
		require.Equal(t, uint64(2), r.term)
		require.Empty(t, drainMessages(r))
	})

	t.Run("candidate_becomes_a_leader_immediately_if_the_cluster_consists_only_one_node", func(t *testing.T) {
		r := createRaft(nil)
		r.peers = nil
		_ = r.becomeFollower(2, 10)

		moveInTime(r, _testElectionTimeout)

		require.Equal(t, uint64(3), r.term)
		require.Equal(t, RoleLeader, r.role)
		require.Equal(t, _localNodeId, r.votedFor)
	})

	t.Run("candidate_sends_vote_requests_to_all_peers_in_the_cluster", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Index: 1, Term: 2}, {Index: 2, Term: 3}})

		r := createRaft(log)
		r.peers = []uint64{10, 11}
		_ = r.becomeFollower(4, 10)

		moveInTime(r, _testElectionTimeout)

		require.Equal(t, RoleCandidate, r.role)
		require.Equal(t, _localNodeId, r.votedFor)
		require.Equal(t, uint64(5), r.term)

		msgs := drainMessages(r)
		require.Len(t, msgs, 2)

		voteReqMsg := pb.Message{Type: pb.MsgVote, From: _localNodeId, To: 10, Term: 5, LogTerm: 3, LogIndex: 2}
		require.Equal(t, voteReqMsg, msgs[0])

		voteReqMsg = pb.Message{Type: pb.MsgVote, From: _localNodeId, To: 11, Term: 5, LogTerm: 3, LogIndex: 2}
		require.Equal(t, voteReqMsg, msgs[1])
	})

	t.Run("candidate_with_an_empty_log_sends_vote_requests_to_all_peers_in_the_cluster", func(t *testing.T) {
		r := createRaft(nil)
		r.peers = []uint64{10, 11}
		_ = r.becomeFollower(4, 10)

		_ = r.step(pb.Message{Type: pb.MsgCampaign})

		msgs := drainMessages(r)
		require.Len(t, msgs, 2)

		voteReqMsg := pb.Message{Type: pb.MsgVote, From: _localNodeId, To: 10, Term: 5, LogTerm: 0, LogIndex: 0}
		require.Equal(t, voteReqMsg, msgs[0])

		voteReqMsg = pb.Message{Type: pb.MsgVote, From: _localNodeId, To: 11, Term: 5, LogTerm: 0, LogIndex: 0}
		require.Equal(t, voteReqMsg, msgs[1])
	})

	t.Run("candidate_starts_another_election_if_it_does_not_get_enough_votes_in_a_timeout", func(t *testing.T) {
		r := createRaft(nil)
		r.peers = []uint64{10, 11}
		r.maxLeaderElectionTimeout = 6
		_ = r.becomeFollower(4, 10)

		// let the election timeout hit and start the first election
		moveInTime(r, 3)

		require.Equal(t, RoleCandidate, r.role)
		require.Equal(t, _localNodeId, r.votedFor)
		require.Equal(t, uint64(5), r.term)
		require.Len(t, drainMessages(r), 2)

		// wait for the leader election timeout to expire again
		moveInTime(r, 3)

		// still a candidate
		require.Equal(t, RoleCandidate, r.role)
		require.Equal(t, _localNodeId, r.votedFor)
		require.Equal(t, uint64(6), r.term)

		msgs := drainMessages(r)
		require.Len(t, msgs, 2)

		// term has to be increased by 2 since two election rounds happened
		require.Equal(t, uint64(6), msgs[0].Term)
		require.Equal(t, uint64(6), msgs[1].Term)
	})

	t.Run("node_ignores_the_vote_request_if_the_term_is_smaller_than_current", func(t *testing.T) {
		r := createRaft(nil)
		r.term = 10
		r.votedFor = None

		_ = r.step(pb.Message{Type: pb.MsgVote, From: 11, To: _localNodeId, Term: 4})

		// did not give the vote away
		require.Equal(t, None, r.votedFor)

		msgs := drainMessages(r)
		require.Empty(t, msgs)
	})

	t.Run("node_rejects_the_vote_if_candidates_last_log_entry_term_is_smaller", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})

		r := createRaft(log)
		_ = r.becomeFollower(2, 10)

		_ = r.step(pb.Message{Type: pb.MsgVote, From: 11, To: _localNodeId, Term: 4, LogTerm: 1, LogIndex: 2})

		// did not give the vote away
		require.Equal(t, None, r.votedFor)

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)

		voteRejectMsg := pb.Message{Type: pb.MsgVoteRes, From: _localNodeId, To: 11, Term: 4, Reject: true}
		require.Equal(t, voteRejectMsg, msgs[0])
	})

	t.Run("node_rejects_the_vote_if_candidates_last_log_entry_index_is_smaller", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}})

		r := createRaft(log)
		_ = r.becomeFollower(2, 10)

		_ = r.step(pb.Message{Type: pb.MsgVote, From: 11, To: _localNodeId, Term: 4, LogTerm: 2, LogIndex: 2})

		// did not give the vote away
		require.Equal(t, None, r.votedFor)

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)

		voteRejectMsg := pb.Message{Type: pb.MsgVoteRes, From: _localNodeId, To: 11, Term: 4, Reject: true}
		require.Equal(t, voteRejectMsg, msgs[0])
	})

	t.Run("node_rejects_the_vote_if_it_has_already_voted_in_that_term", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}})

		r := createRaft(log)
		r.term = 2

		// first vote from node #11
		_ = r.step(pb.Message{Type: pb.MsgVote, From: 11, To: _localNodeId, Term: 3, LogTerm: 2, LogIndex: 3})

		// granted the vote for node #11
		require.Equal(t, uint64(3), r.term)
		require.Equal(t, uint64(11), r.votedFor)

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)
		require.Equal(t, pb.Message{Type: pb.MsgVoteRes, From: _localNodeId, To: 11, Term: 3, Reject: false}, msgs[0])

		// another node #12 wants a vote for the same term
		_ = r.step(pb.Message{Type: pb.MsgVote, From: 12, To: _localNodeId, Term: 3, LogTerm: 2, LogIndex: 3})

		// vote stays for the first node #11
		require.Equal(t, uint64(3), r.term)
		require.Equal(t, uint64(11), r.votedFor)

		msgs = drainMessages(r)
		require.Len(t, msgs, 1)
		require.Equal(t, pb.Message{Type: pb.MsgVoteRes, From: _localNodeId, To: 12, Term: 3, Reject: true}, msgs[0])
	})

	t.Run("node_accepts_the_vote_from_the_same_node_multiple_times_for_the_same_term", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}})

		r := createRaft(log)
		r.term = 2

		// first vote from node #11
		_ = r.step(pb.Message{Type: pb.MsgVote, From: 11, To: _localNodeId, Term: 3, LogTerm: 2, LogIndex: 3})

		// granted the vote for node #11
		require.Equal(t, uint64(3), r.term)
		require.Equal(t, uint64(11), r.votedFor)

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)
		require.Equal(t, pb.Message{Type: pb.MsgVoteRes, From: _localNodeId, To: 11, Term: 3, Reject: false}, msgs[0])

		// duplicate vote from node #11
		_ = r.step(pb.Message{Type: pb.MsgVote, From: 11, To: _localNodeId, Term: 3, LogTerm: 2, LogIndex: 3})

		// results did not change
		require.Equal(t, uint64(3), r.term)
		require.Equal(t, uint64(11), r.votedFor)

		msgs = drainMessages(r)
		require.Len(t, msgs, 1)
		require.Equal(t, pb.Message{Type: pb.MsgVoteRes, From: _localNodeId, To: 11, Term: 3, Reject: false}, msgs[0])
	})

	t.Run("node_resets_its_leader_election_timeout_when_giving_the_vote_away", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3}})

		r := createRaft(log)
		_ = r.becomeFollower(2, 10)

		moveInTime(r, _testElectionTimeout-1)
		require.Equal(t, _testElectionTimeout-1, r.ticksFromLeaderAction)

		// vote which should be rejected because of stale log
		_ = r.step(pb.Message{Type: pb.MsgVote, From: 10, To: _localNodeId, Term: 3, LogTerm: 1, LogIndex: 2})

		// vote was not granted and election timeout was reset because term changed
		require.Equal(t, None, r.votedFor)
		require.Equal(t, 0, r.ticksFromLeaderAction)

		moveInTime(r, _testElectionTimeout-1)
		require.Equal(t, _testElectionTimeout-1, r.ticksFromLeaderAction)

		// vote which should be rejected because of stale log
		_ = r.step(pb.Message{Type: pb.MsgVote, From: 11, To: _localNodeId, Term: 3, LogTerm: 1, LogIndex: 2})

		// vote was not granted and election timeout was not reset because term did not change and vote was not granted
		require.Equal(t, None, r.votedFor)
		require.Equal(t, _testElectionTimeout-1, r.ticksFromLeaderAction)

		// vote which will be accepted
		_ = r.step(pb.Message{Type: pb.MsgVote, From: 12, To: _localNodeId, Term: 3, LogTerm: 2, LogIndex: 4})

		// vote was granted and election timeout was reset
		require.Equal(t, uint64(12), r.votedFor)
		require.Equal(t, 0, r.ticksFromLeaderAction)
	})

	t.Run("candidate_becomes_a_follower_if_receives_reject_with_higher_term", func(t *testing.T) {
		r := createRaft(nil)
		r.peers = []uint64{10, 11}
		_ = r.becomeFollower(2, 10)

		_ = r.step(pb.Message{Type: pb.MsgCampaign})
		_ = drainMessages(r)

		// peer rejected election with higher term
		_ = r.step(pb.Message{Type: pb.MsgVoteRes, From: 11, To: _localNodeId, Term: 4, Reject: true})

		require.Equal(t, RoleFollower, r.role)
		require.Equal(t, None, r.votedFor)
		require.Equal(t, uint64(4), r.term)
	})

	t.Run("node_votes_for_a_candidate_if_all_conditions_satisfied", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}})

		r := createRaft(log)
		r.term = 1

		_ = r.step(pb.Message{Type: pb.MsgVote, From: 11, To: _localNodeId, Term: 4, LogTerm: 3, LogIndex: 5})

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)

		voteGrantedMsg := pb.Message{Type: pb.MsgVoteRes, From: _localNodeId, To: 11, Term: 4, Reject: false}
		require.Equal(t, voteGrantedMsg, msgs[0])

		require.Equal(t, RoleFollower, r.role)
		require.Equal(t, uint64(4), r.term)
		require.Equal(t, uint64(11), r.votedFor)
	})

	t.Run("node_can_get_a_vote_even_with_an_empty_log", func(t *testing.T) {
		r := createRaft(nil)
		r.term = 1

		_ = r.step(pb.Message{Type: pb.MsgVote, From: 11, To: _localNodeId, Term: 4, LogTerm: 0, LogIndex: 0})

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)

		voteGrantedMsg := pb.Message{Type: pb.MsgVoteRes, From: _localNodeId, To: 11, Term: 4, Reject: false}
		require.Equal(t, voteGrantedMsg, msgs[0])

		require.Equal(t, RoleFollower, r.role)
		require.Equal(t, uint64(4), r.term)
		require.Equal(t, uint64(11), r.votedFor)
	})

	t.Run("candidate_is_elected_when_quorum_of_votes_is_received", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})

		r := createRaft(log)
		_ = r.becomeFollower(1, 5)
		r.peers = []uint64{5, 6, 7, 8}

		_ = r.step(pb.Message{Type: pb.MsgCampaign})

		msgs := drainMessages(r)
		require.Len(t, msgs, 4)
		require.Equal(t, RoleCandidate, r.role)
		require.Equal(t, _localNodeId, r.votedFor)

		// grant vote from node #5
		_ = r.step(pb.Message{Type: pb.MsgVoteRes, From: 5, To: _localNodeId, Term: 2, Reject: false})

		// should still be a candidate because only 2/5 votes received
		require.Equal(t, RoleCandidate, r.role)

		// grant vote from node #6
		_ = r.step(pb.Message{Type: pb.MsgVoteRes, From: 6, To: _localNodeId, Term: 2, Reject: false})

		// should become a leader because received 3/5 votes
		require.Equal(t, RoleLeader, r.role)
		require.Equal(t, _localNodeId, r.votedFor)
	})

	t.Run("candidate reverts back to follower when it looses election", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})

		r := createRaft(log)
		_ = r.becomeFollower(1, 5)
		r.peers = []uint64{5, 6, 7, 8}

		_ = r.step(pb.Message{Type: pb.MsgCampaign})

		msgs := drainMessages(r)
		require.Len(t, msgs, 4)
		require.Equal(t, RoleCandidate, r.role)

		// grant vote from node #5
		_ = r.step(pb.Message{Type: pb.MsgVoteRes, From: 5, To: _localNodeId, Term: 2, Reject: false})

		// should still be a candidate because only 2/5 votes received
		require.Equal(t, RoleCandidate, r.role)

		// reject from all other three nodes
		_ = r.step(pb.Message{Type: pb.MsgVoteRes, From: 6, To: _localNodeId, Term: 2, Reject: true})
		_ = r.step(pb.Message{Type: pb.MsgVoteRes, From: 7, To: _localNodeId, Term: 2, Reject: true})
		_ = r.step(pb.Message{Type: pb.MsgVoteRes, From: 8, To: _localNodeId, Term: 2, Reject: true})

		// should revert to follower because lost the election
		require.Equal(t, RoleFollower, r.role)
		require.Equal(t, _localNodeId, r.votedFor)
	})

	t.Run("leaders_does_all_required_actions_on_won_election", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2}})

		r := createRaft(log)
		r.term = 4
		r.peers = []uint64{10, 11}
		r.role = RoleCandidate
		r.votedFor = _localNodeId
		r.raftLog.commitIndex = 2

		_ = r.becomeLeader()

		require.Equal(t, RoleLeader, r.role)
		require.Equal(t, _localNodeId, r.votedFor)
		require.Equal(t, uint64(4), r.term)

		// leader resets matchIndex
		require.Equal(t, map[uint64]uint64{10: 0, 11: 0}, r.matchIndex)

		// leader resets next index
		require.Equal(t, map[uint64]uint64{10: 3, 11: 3}, r.nextIndex)

		// leader appends a dummy entry
		require.Len(t, r.raftLog.unstableEntries(), 1)
		require.Equal(t, pb.Entry{Term: 4, Index: 3}, r.raftLog.unstableEntries()[0])

		// leader broadcasts the dummy append entry to all peers
		msgs := drainMessages(r)
		require.Len(t, msgs, 2)

		appMsg := pb.Message{
			Type:        pb.MsgApp,
			From:        _localNodeId,
			To:          10,
			Term:        4,
			Entries:     []pb.Entry{{Term: 4, Index: 3}}, // replicates only the last dummy entry
			LogTerm:     3,
			LogIndex:    2,
			CommitIndex: 2,
		}
		require.Equal(t, appMsg, msgs[0])

		appMsg.To = 11
		require.Equal(t, appMsg, msgs[1])
	})

	t.Run("leaders_appends/broadcasts_first_ever_entry", func(t *testing.T) {
		r := createRaft(nil)
		r.term = 1
		r.peers = []uint64{10, 11}
		r.role = RoleCandidate
		r.raftLog.commitIndex = 0

		_ = r.becomeLeader()
		require.Equal(t, RoleLeader, r.role)

		require.Len(t, allEntries(r), 1)
		require.Equal(t, pb.Entry{Term: 1, Index: 1}, allEntries(r)[0])

		msgs := drainMessages(r)
		require.Len(t, msgs, 2)

		appMsg := pb.Message{
			Type:        pb.MsgApp,
			From:        _localNodeId,
			To:          10,
			Term:        1,
			Entries:     []pb.Entry{{Term: 1, Index: 1}},
			LogTerm:     0,
			LogIndex:    0,
			CommitIndex: 0,
		}
		require.Equal(t, appMsg, msgs[0])

		appMsg.To = 11
		require.Equal(t, appMsg, msgs[1])
	})

	t.Run("follower_handle_simple_successful_append_request", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2}})

		r := createRaft(log)
		r.term = 4
		r.raftLog.commitIndex = 1

		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        10,
			To:          _localNodeId,
			Term:        5,
			Entries:     []pb.Entry{{Term: 4, Index: 3}},
			LogTerm:     3,
			LogIndex:    2,
			CommitIndex: 3,
		})

		require.Equal(t, uint64(5), r.term)
		require.Equal(t, []pb.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2}, {Term: 4, Index: 3}}, allEntries(r))
		require.Equal(t, uint64(3), r.raftLog.commitIndex)

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)

		msgAppRes := pb.Message{Type: pb.MsgAppRes, From: _localNodeId, To: 10, Term: 5, LogIndex: 3, Reject: false}
		require.Equal(t, msgAppRes, msgs[0])
	})

	t.Run("follower_resets_voted_for_on_higher_term_request", func(t *testing.T) {
		r := createRaft(nil)
		r.term = 4
		r.votedFor = 11

		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        10,
			To:          _localNodeId,
			Term:        5,
			Entries:     []pb.Entry{{Term: 1, Index: 1}},
			LogTerm:     0,
			LogIndex:    0,
			CommitIndex: 3,
		})

		require.Equal(t, uint64(5), r.term)
		require.Equal(t, None, r.votedFor)
	})

	t.Run("follower_resets_leader_election_timeout_on_append_entries_request", func(t *testing.T) {
		r := createRaft(nil)
		_ = r.becomeFollower(4, 10)

		moveInTime(r, _testElectionTimeout-1)

		// still a follower
		require.Equal(t, _testElectionTimeout-1, r.ticksFromLeaderAction)
		require.Equal(t, RoleFollower, r.role)

		// message from the leader
		_ = r.step(pb.Message{Type: pb.MsgApp, From: 10, To: _localNodeId, Term: 4})

		// timeout is reset
		require.Equal(t, 0, r.ticksFromLeaderAction)
	})

	t.Run("follower_rejects_append_request_if_previous_log_index_is_bigger_than_current_log", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2}})

		r := createRaft(log)
		_ = r.becomeFollower(4, 10)

		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        10,
			To:          _localNodeId,
			Term:        5,
			Entries:     []pb.Entry{{Term: 7, Index: 11}},
			LogTerm:     6,
			LogIndex:    10,
			CommitIndex: 3,
		})

		// log did not change
		require.Equal(t, []pb.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2}}, allEntries(r))

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)
		require.Equal(t, pb.Message{Type: pb.MsgAppRes, Term: 5, To: 10, From: _localNodeId, Reject: true}, msgs[0])
	})

	t.Run("follower_rejects_append_request_if_previous_log_term_is_not_correct", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 3, Index: 2}})

		r := createRaft(log)
		_ = r.becomeFollower(4, 10)

		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        10,
			To:          _localNodeId,
			Term:        5,
			Entries:     []pb.Entry{{Term: 4, Index: 3}},
			LogTerm:     4,
			LogIndex:    2,
			CommitIndex: 1,
		})

		// log did not change
		require.Equal(t, []pb.Entry{{Term: 1, Index: 1}, {Term: 3, Index: 2}}, allEntries(r))

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)
		require.Equal(t, pb.Message{Type: pb.MsgAppRes, Term: 5, To: 10, From: _localNodeId, Reject: true}, msgs[0])
	})

	t.Run("candidate_resets_leader_election_timeout_on_append_entries_request", func(t *testing.T) {
		r := createRaft(nil)
		r.peers = []uint64{10, 11}
		_ = r.becomeFollower(3, 10)

		_ = r.step(pb.Message{Type: pb.MsgCampaign})

		moveInTime(r, _testElectionTimeout-1)

		// still a candidate
		require.Equal(t, _testElectionTimeout-1, r.ticksFromLeaderAction)
		require.Equal(t, RoleCandidate, r.role)

		// message from the leader
		_ = r.step(pb.Message{Type: pb.MsgApp, From: 10, To: _localNodeId, Term: 4})

		// timeout is reset
		require.Equal(t, 0, r.ticksFromLeaderAction)
	})

	t.Run("candidate_reverts_to_follower_if_receives_append_in_the_same_term", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2}})

		r := createRaft(log)
		r.peers = []uint64{10, 11}
		_ = r.becomeFollower(3, 10)

		_ = r.step(pb.Message{Type: pb.MsgCampaign})
		drainMessages(r)

		require.Equal(t, uint64(4), r.term)
		require.Equal(t, RoleCandidate, r.role)
		require.Equal(t, None, r.leaderId)
		require.Equal(t, _localNodeId, r.votedFor)

		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        10,
			To:          _localNodeId,
			Term:        4,
			Entries:     []pb.Entry{{Term: 4, Index: 3}},
			LogTerm:     3,
			LogIndex:    2,
			CommitIndex: 3,
		})

		// reverted to follower
		require.Equal(t, uint64(4), r.term)
		require.Equal(t, RoleFollower, r.role)
		require.Equal(t, _localNodeId, r.votedFor)
		require.Equal(t, uint64(10), r.leaderId)

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)

		msgAppRes := pb.Message{Type: pb.MsgAppRes, From: _localNodeId, To: 10, Term: 4, LogIndex: 3, Reject: false}
		require.Equal(t, msgAppRes, msgs[0])
	})

	t.Run("follower_tries_to_append_first_ever_entry_in_the_log", func(t *testing.T) {
		r := createRaft(nil)
		r.raftLog.commitIndex = 0
		_ = r.becomeFollower(1, 10)

		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        10,
			To:          _localNodeId,
			Term:        1,
			Entries:     []pb.Entry{{Term: 1, Index: 1}},
			LogTerm:     0,
			LogIndex:    0,
			CommitIndex: 0,
		})

		require.Equal(t, []pb.Entry{{Term: 1, Index: 1}}, allEntries(r))

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)
		require.False(t, msgs[0].Reject)
	})

	t.Run("leader_updates_required_data_on_successful_append_responses_and_broadcasts_new_commit_index", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2}})

		r := createRaft(log)
		r.term = 3
		r.peers = []uint64{10, 11, 12}
		r.role = RoleCandidate
		r.raftLog.commitIndex = 2

		_ = r.becomeLeader()
		_ = drainMessages(r)

		// starting indexes
		require.Equal(t, map[uint64]uint64{10: 0, 11: 0, 12: 0}, r.matchIndex)
		require.Equal(t, map[uint64]uint64{10: 3, 11: 3, 12: 3}, r.nextIndex)

		// node #10 acknowledges the append request
		_ = r.step(pb.Message{Type: pb.MsgAppRes, Term: 3, From: 10, To: _localNodeId, LogIndex: 3, Reject: false})

		// required indexes moved
		require.Equal(t, map[uint64]uint64{10: 3, 11: 0, 12: 0}, r.matchIndex)
		require.Equal(t, map[uint64]uint64{10: 4, 11: 3, 12: 3}, r.nextIndex)

		// commit index not moved yet because not a quorum acknowledged it yet
		require.Equal(t, uint64(2), r.raftLog.commitIndex)

		// node #12 acknowledges the append request
		_ = r.step(pb.Message{Type: pb.MsgAppRes, Term: 3, From: 12, To: _localNodeId, LogIndex: 3, Reject: false})

		// required indexes moved
		require.Equal(t, map[uint64]uint64{10: 3, 11: 0, 12: 3}, r.matchIndex)
		require.Equal(t, map[uint64]uint64{10: 4, 11: 3, 12: 4}, r.nextIndex)

		// commit index updated because quorum of nodes has nodes up to index 3 replicated
		require.Equal(t, uint64(3), r.raftLog.commitIndex)

		// node broadcasts append messages to all nodes because commit index was moved
		msgs := drainMessages(r)
		require.Len(t, msgs, 3)
		for i, peer := range []uint64{10, 11, 12} {
			require.Equal(t, pb.MsgApp, msgs[i].Type)
			require.Equal(t, uint64(3), msgs[i].Term)
			require.Equal(t, peer, msgs[i].To)
			require.Equal(t, uint64(3), msgs[i].CommitIndex)
		}

		// another late message arrives with smaller log index from node #10
		_ = r.step(pb.Message{Type: pb.MsgAppRes, Term: 3, From: 10, To: _localNodeId, LogIndex: 2, Reject: false})

		// indexes are staying the same
		require.Equal(t, map[uint64]uint64{10: 3, 11: 0, 12: 3}, r.matchIndex)
		require.Equal(t, map[uint64]uint64{10: 4, 11: 3, 12: 4}, r.nextIndex)

		// no messages were sent on stale message
		require.Empty(t, drainMessages(r))
	})

	t.Run("leader_commits_new_entries_only_from_its_own_term", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 2, Index: 1}, {Term: 2, Index: 2}})

		r := createRaft(log)
		r.term = 4
		r.peers = []uint64{10, 11}
		r.role = RoleCandidate
		r.raftLog.commitIndex = 0

		_ = r.becomeLeader()
		_ = drainMessages(r)

		// starting indexes
		require.Equal(t, map[uint64]uint64{10: 0, 11: 0}, r.matchIndex)
		require.Equal(t, map[uint64]uint64{10: 3, 11: 3}, r.nextIndex)

		// starting log after won election
		require.Equal(t, []pb.Entry{{Term: 2, Index: 1}, {Term: 2, Index: 2}, {Term: 4, Index: 3}}, allEntries(r))

		// ...some chit chatter is going in the cluster at this point...

		// node #10 acknowledges the append request for log up to index 2
		_ = r.step(pb.Message{Type: pb.MsgAppRes, Term: 4, From: 10, To: _localNodeId, LogIndex: 2, Reject: false})

		// commit index not moved yet because leader cannot commit entries not from its term
		require.Equal(t, uint64(0), r.raftLog.commitIndex)

		// node #11 acknowledges the append request for log up to index 2
		_ = r.step(pb.Message{Type: pb.MsgAppRes, Term: 4, From: 11, To: _localNodeId, LogIndex: 2, Reject: false})

		// matchIndex is moved for both nodes
		require.Equal(t, map[uint64]uint64{10: 2, 11: 2}, r.matchIndex)
		require.Equal(t, map[uint64]uint64{10: 3, 11: 3}, r.nextIndex)

		// however, no commit yet
		require.Equal(t, uint64(0), r.raftLog.commitIndex)

		// node #10 acknowledges the append request for log up to index 3
		_ = r.step(pb.Message{Type: pb.MsgAppRes, Term: 4, From: 10, To: _localNodeId, LogIndex: 3, Reject: false})

		// log is committed, since entry #3 is from current leader's term
		require.Equal(t, uint64(3), r.raftLog.commitIndex)

		// leader broadcasts append messages to all nodes because commit index was moved
		msgs := drainMessages(r)
		require.Len(t, msgs, 2)
		for i, peer := range []uint64{10, 11} {
			require.Equal(t, pb.MsgApp, msgs[i].Type)
			require.Equal(t, uint64(4), msgs[i].Term)
			require.Equal(t, peer, msgs[i].To)
			require.Equal(t, uint64(3), msgs[i].CommitIndex)
		}
	})

	t.Run("leader_updates_required_data_on_failed_append_responses_and_sends_another_requests", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2}, {Term: 3, Index: 3}})

		r := createRaft(log)
		r.term = 3
		r.peers = []uint64{10, 11}
		r.role = RoleCandidate
		r.raftLog.commitIndex = 2

		_ = r.becomeLeader()
		_ = drainMessages(r)

		// starting indexes
		require.Equal(t, map[uint64]uint64{10: 0, 11: 0}, r.matchIndex)
		require.Equal(t, map[uint64]uint64{10: 4, 11: 4}, r.nextIndex)

		// node #10 rejects the request
		_ = r.step(pb.Message{Type: pb.MsgAppRes, Term: 3, From: 10, To: _localNodeId, Reject: true})

		// indexes are updated
		require.Equal(t, map[uint64]uint64{10: 0, 11: 0}, r.matchIndex)
		require.Equal(t, map[uint64]uint64{10: 3, 11: 4}, r.nextIndex)

		// commit index is not updated
		require.Equal(t, uint64(2), r.raftLog.commitIndex)

		msgs := drainMessages(r)
		require.Len(t, msgs, 1)

		// new updated append message is sent
		appMsg := pb.Message{
			Type:        pb.MsgApp,
			Term:        3,
			To:          10,
			From:        _localNodeId,
			LogIndex:    2,
			LogTerm:     3,
			Entries:     []pb.Entry{{Term: 3, Index: 3}, {Term: 3, Index: 4}},
			CommitIndex: 2,
		}
		require.Equal(t, appMsg, msgs[0])

		// node #10 rejects the request again
		_ = r.step(pb.Message{Type: pb.MsgAppRes, Term: 3, From: 10, To: _localNodeId, Reject: true})

		// indexes are updated
		require.Equal(t, map[uint64]uint64{10: 0, 11: 0}, r.matchIndex)
		require.Equal(t, map[uint64]uint64{10: 2, 11: 4}, r.nextIndex)

		// commit index is not updated
		require.Equal(t, uint64(2), r.raftLog.commitIndex)

		msgs = drainMessages(r)
		require.Len(t, msgs, 1)

		// new updated append message is sent again
		appMsg = pb.Message{
			Type:        pb.MsgApp,
			Term:        3,
			To:          10,
			From:        _localNodeId,
			LogIndex:    1,
			LogTerm:     3,
			Entries:     []pb.Entry{{Term: 3, Index: 2}, {Term: 3, Index: 3}, {Term: 3, Index: 4}},
			CommitIndex: 2,
		}
		require.Equal(t, appMsg, msgs[0])

		// now node #11 rejects the request
		_ = r.step(pb.Message{Type: pb.MsgAppRes, Term: 3, From: 11, To: _localNodeId, Reject: true})

		// indexes are updated
		require.Equal(t, map[uint64]uint64{10: 0, 11: 0}, r.matchIndex)
		require.Equal(t, map[uint64]uint64{10: 2, 11: 3}, r.nextIndex)

		// commit index is not updated
		require.Equal(t, uint64(2), r.raftLog.commitIndex)

		msgs = drainMessages(r)
		require.Len(t, msgs, 1)

		// new updated append message is sent
		appMsg = pb.Message{
			Type:        pb.MsgApp,
			Term:        3,
			To:          11,
			From:        _localNodeId,
			LogIndex:    2,
			LogTerm:     3,
			Entries:     []pb.Entry{{Term: 3, Index: 3}, {Term: 3, Index: 4}},
			CommitIndex: 2,
		}
		require.Equal(t, appMsg, msgs[0])
	})

	t.Run("leader_periodically_broadcasts_heartbeat_appends_to_all_peers", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2}, {Term: 3, Index: 3}})

		r := createRaft(log)
		r.term = 3
		r.peers = []uint64{10, 11}
		r.role = RoleCandidate

		_ = r.becomeLeader()
		_ = drainMessages(r)

		// setup test indexes
		r.nextIndex = map[uint64]uint64{10: 2, 11: 3}

		moveInTime(r, _testHeartBeatTimeout)

		// heartbeat appends should be sent to all peers
		msgs := drainMessages(r)
		require.Len(t, msgs, 2)

		appMsg := pb.Message{
			Type:        pb.MsgApp,
			Term:        3,
			To:          10,
			From:        _localNodeId,
			LogIndex:    1,
			LogTerm:     3,
			Entries:     []pb.Entry{{Term: 3, Index: 2}, {Term: 3, Index: 3}, {Term: 3, Index: 4}},
			CommitIndex: 0,
		}
		require.Equal(t, appMsg, msgs[0])

		appMsg = pb.Message{
			Type:        pb.MsgApp,
			Term:        3,
			To:          11,
			From:        _localNodeId,
			LogIndex:    2,
			LogTerm:     3,
			Entries:     []pb.Entry{{Term: 3, Index: 3}, {Term: 3, Index: 4}},
			CommitIndex: 0,
		}
		require.Equal(t, appMsg, msgs[1])

		// heartbeat ticks should be reset
		require.Equal(t, 0, r.ticksFromHeartBeat)
	})

	t.Run("single_node_cluster_leader_commits_entries_immediately", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 3, Index: 1}, {Term: 3, Index: 2}, {Term: 3, Index: 3}})

		r := createRaft(log)
		r.term = 3
		r.peers = []uint64{}
		r.role = RoleCandidate

		r.raftLog.committedTo(3)
		r.raftLog.appliedTo(3)

		_ = r.becomeLeader()

		require.Empty(t, drainMessages(r))

		err := r.step(pb.Message{Type: pb.MsgPropose, Term: 3, Entries: []pb.Entry{{Data: []byte("hello there")}}})
		require.NoError(t, err)

		require.Empty(t, drainMessages(r))

		// term start entry + proposed entry
		require.Equal(t, uint64(5), r.raftLog.commitIndex)

		notAppliedEntries, err := r.raftLog.notAppliedEntries()
		require.NoError(t, err)
		require.Equal(t, []pb.Entry{{Term: 3, Index: 4}, {Term: 3, Index: 5, Data: []byte("hello there")}}, notAppliedEntries)
	})

	t.Run("part_of_followers_unstable_log_is_overridden_on_leader_change", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}})

		r := createRaft(log)
		_ = r.becomeFollower(2, 10)

		// one leader appends its log
		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        10,
			To:          _localNodeId,
			Term:        4,
			Entries:     []pb.Entry{{Term: 2, Index: 2}, {Term: 3, Index: 3}, {Term: 3, Index: 4}},
			LogTerm:     1,
			LogIndex:    1,
			CommitIndex: 1,
		})

		// log changed
		require.Equal(t, []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}, {Term: 3, Index: 4}}, allEntries(r))

		// another leader from next term appends conflicting log
		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        10,
			To:          _localNodeId,
			Term:        5,
			Entries:     []pb.Entry{{Term: 2, Index: 2}, {Term: 5, Index: 3}, {Term: 5, Index: 4}},
			LogTerm:     1,
			LogIndex:    1,
			CommitIndex: 1,
		})

		// log matches new leader's
		require.Equal(t, []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 5, Index: 3}, {Term: 5, Index: 4}}, allEntries(r))
	})

	t.Run("whole_followers_unstable_log_is_overridden_on_leader_change", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})

		r := createRaft(log)
		_ = r.becomeFollower(2, 10)

		// one leader appends its log
		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        10,
			To:          _localNodeId,
			Term:        4,
			Entries:     []pb.Entry{{Term: 3, Index: 3}, {Term: 3, Index: 4}},
			LogTerm:     2,
			LogIndex:    2,
			CommitIndex: 1,
		})

		// log changed
		require.Equal(t, []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}, {Term: 3, Index: 4}}, allEntries(r))

		// another leader from next term appends conflicting log
		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        10,
			To:          _localNodeId,
			Term:        5,
			Entries:     []pb.Entry{{Term: 5, Index: 2}, {Term: 5, Index: 3}, {Term: 5, Index: 4}},
			LogTerm:     1,
			LogIndex:    1,
			CommitIndex: 1,
		})

		// log matches new leader's
		require.Equal(t, []pb.Entry{{Term: 1, Index: 1}, {Term: 5, Index: 2}, {Term: 5, Index: 3}, {Term: 5, Index: 4}}, allEntries(r))
	})

	t.Run("follower_rejects_proposals", func(t *testing.T) {
		r := createRaft(nil)
		_ = r.becomeFollower(2, 10)

		err := r.step(pb.Message{Type: pb.MsgPropose, Entries: []pb.Entry{{Data: []byte("test")}}})
		require.Equal(t, ErrNotLeader, err)
		require.Empty(t, allEntries(r))
	})

	t.Run("candidate_rejects_proposal_value", func(t *testing.T) {
		r := createRaft(nil)
		_ = r.becomeFollower(3, 10)
		_ = r.becomeCandidate()

		err := r.step(pb.Message{Type: pb.MsgPropose, Entries: []pb.Entry{{Data: []byte("test")}}})
		require.Equal(t, RoleCandidate, r.role)
		require.Equal(t, ErrNotLeader, err)
		require.Empty(t, allEntries(r))
	})

	t.Run("leader_accepts_proposal_value_appends_it_to_its_log_and_broadcasts_append", func(t *testing.T) {
		r := createRaft(nil)
		r.term = 2
		r.peers = []uint64{10, 11}
		r.role = RoleCandidate

		_ = r.becomeLeader()
		_ = drainMessages(r)

		err := r.step(pb.Message{Type: pb.MsgPropose, Entries: []pb.Entry{{Data: []byte("test")}}})
		require.NoError(t, err)

		require.Equal(t, []pb.Entry{{Term: 2, Index: 1}, {Term: 2, Index: 2, Data: []byte("test")}}, allEntries(r))

		msgs := drainMessages(r)
		require.Len(t, msgs, 2)

		for i, peer := range []uint64{10, 11} {
			appMsg := pb.Message{
				Type:        pb.MsgApp,
				From:        _localNodeId,
				To:          peer,
				Term:        2,
				Entries:     []pb.Entry{{Term: 2, Index: 1}, {Term: 2, Index: 2, Data: []byte("test")}},
				LogTerm:     0,
				LogIndex:    0,
				CommitIndex: 0,
			}
			require.Equal(t, appMsg, msgs[i])
		}
	})

	t.Run("node_resets_leaderId_when_becoming_a_candidate", func(t *testing.T) {
		r := createRaft(nil)
		r.peers = []uint64{10, 11}
		_ = r.becomeFollower(2, 10)

		require.Equal(t, uint64(10), r.leaderId)

		moveInTime(r, _testElectionTimeout)

		require.Equal(t, RoleCandidate, r.role)
		require.Equal(t, None, r.leaderId)
	})

	t.Run("node_resets_leaderId_on_vote_request", func(t *testing.T) {
		r := createRaft(nil)
		_ = r.becomeFollower(2, 10)

		require.Equal(t, uint64(10), r.leaderId)
		_ = r.step(pb.Message{Type: pb.MsgVote, To: _localNodeId, From: 10, Term: 5, LogTerm: 3, LogIndex: 2})
		require.Equal(t, None, r.leaderId)
	})

	t.Run("node_sets_leaderId_when_receiving_append_entries_request", func(t *testing.T) {
		r := createRaft(nil)
		_ = r.becomeFollower(2, 10)

		_ = r.step(pb.Message{
			Type:        pb.MsgApp,
			From:        12,
			To:          _localNodeId,
			Term:        4,
			LogTerm:     0,
			LogIndex:    0,
			CommitIndex: 2,
		})

		require.Equal(t, uint64(12), r.leaderId)
	})

	t.Run("leader_sets_leaderId_on_becoming_the_leader", func(t *testing.T) {
		r := createRaft(nil)
		r.peers = nil // election should be won right after it is started
		_ = r.becomeFollower(2, 10)

		moveInTime(r, _testElectionTimeout)

		require.Equal(t, RoleLeader, r.role)
		require.Equal(t, _localNodeId, r.leaderId)
	})

	t.Run("leader_can_handle_multiple_append_rejects", func(t *testing.T) {
		log := newInMemoryStorage([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})

		r := createRaft(log)
		r.term = 3
		r.peers = []uint64{10}
		r.role = RoleCandidate

		_ = r.becomeLeader()
		_ = drainMessages(r)

		baseAppMsg := pb.Message{
			Type:        pb.MsgApp,
			From:        _localNodeId,
			To:          10,
			Term:        3,
			CommitIndex: 0,
		}

		ttable := []struct {
			expectLogTerm  uint64
			expectLogIndex uint64
			expectEntries  []pb.Entry
		}{
			{
				expectLogTerm:  1,
				expectLogIndex: 1,
				expectEntries:  []pb.Entry{{Term: 2, Index: 2}, {Term: 3, Index: 3}},
			},

			// reject initial message multiple times to simulate bad network and unlucky calls
			{
				expectLogTerm:  0,
				expectLogIndex: 0,
				expectEntries:  []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}},
			},
			{
				expectLogTerm:  0,
				expectLogIndex: 0,
				expectEntries:  []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}},
			},
			{
				expectLogTerm:  0,
				expectLogIndex: 0,
				expectEntries:  []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}},
			},
		}

		for i, tt := range ttable {
			t.Run(fmt.Sprintf("case_%v", i), func(t *testing.T) {
				err := r.step(pb.Message{Type: pb.MsgAppRes, From: 10, To: _localNodeId, Reject: true})
				require.NoError(t, err)

				msgs := drainMessages(r)
				require.Len(t, msgs, 1)

				msg := baseAppMsg
				msg.LogTerm = tt.expectLogTerm
				msg.LogIndex = tt.expectLogIndex
				msg.Entries = tt.expectEntries

				require.Equal(t, msg, msgs[0])
			})
		}
	})
}

func TestParamsValidation(t *testing.T) {
	correctParams := Params{
		ID:                       4,
		StateStorage:             newInMemoryStorage([]pb.Entry{}),
		LogStorage:               newInMemoryStorage([]pb.Entry{}),
		MaxLeaderElectionTimeout: 10,
		HeartBeatTimeout:         2,
		Peers:                    []uint64{1, 2},
		Logger:                   logging.NewLogger("raft", logging.Debug),
	}

	t.Run("correct_params_should_not_return_error", func(t *testing.T) {
		err := correctParams.validate()
		require.NoError(t, err)
	})

	ttable := []struct {
		name        string
		breakParams func(p *Params)

		expectErrMsg string
	}{
		{
			name: "id_is_zero",
			breakParams: func(p *Params) {
				p.ID = 0
			},
			expectErrMsg: "raft ID has to be a greater than 0",
		},
		{
			name: "state_storage_is_nil",
			breakParams: func(p *Params) {
				p.StateStorage = nil
			},
			expectErrMsg: "state storage cannot be nil",
		},
		{
			name: "log_storage_is_nil",
			breakParams: func(p *Params) {
				p.LogStorage = nil
			},
			expectErrMsg: "log storage cannot be nil",
		},
		{
			name: "heart_beat_timeout_is_zero",
			breakParams: func(p *Params) {
				p.HeartBeatTimeout = 0
			},
			expectErrMsg: "heart beat timeout has to be a positive number greater than 0",
		},
		{
			name: "heart_beat_timeout_is_negative",
			breakParams: func(p *Params) {
				p.HeartBeatTimeout = -50
			},
			expectErrMsg: "heart beat timeout has to be a positive number greater than 0",
		},
		{
			name: "max_leader_election_timeout_timeout_is_zero",
			breakParams: func(p *Params) {
				p.MaxLeaderElectionTimeout = 0
			},
			expectErrMsg: "max leader election timeout has to be a positive number greater than 0",
		},
		{
			name: "max_leader_election_is_negative",
			breakParams: func(p *Params) {
				p.MaxLeaderElectionTimeout = -9
			},
			expectErrMsg: "max leader election timeout has to be a positive number greater than 0",
		},
		{
			name: "heart_beat_timeout_is_equal_to_max_leader_election",
			breakParams: func(p *Params) {
				p.HeartBeatTimeout = 10
				p.MaxLeaderElectionTimeout = 10
			},
			expectErrMsg: "max leader election timeout has to be greater than heart beat timeout",
		},
		{
			name: "heart_beat_timeout_is_greater_to_max_leader_election",
			breakParams: func(p *Params) {
				p.HeartBeatTimeout = 100
				p.MaxLeaderElectionTimeout = 10
			},
			expectErrMsg: "max leader election timeout has to be greater than heart beat timeout",
		},
	}

	for _, tt := range ttable {
		t.Run(tt.name, func(t *testing.T) {
			copied := correctParams
			tt.breakParams(&copied)

			err := copied.validate()
			require.Equal(t, tt.expectErrMsg, err.Error())
		})
	}
}

func TestCreateRaft(t *testing.T) {
	params := Params{
		ID:                       5,
		StateStorage:             newInMemoryStorage([]pb.Entry{}),
		LogStorage:               newInMemoryStorage([]pb.Entry{}),
		MaxLeaderElectionTimeout: 10,
		HeartBeatTimeout:         3,
		Peers:                    []uint64{1, 2, 3},
		Logger:                   logging.System,
	}

	t.Run("default_initial_state_is_set_if_none_is_persisted", func(t *testing.T) {
		copied := params
		copied.StateStorage = newInMemoryStorage(nil)

		r, err := newRaft(copied)
		require.NoError(t, err)
		require.Equal(t, uint64(1), r.term)
		require.Equal(t, None, r.votedFor)
	})

	t.Run("persisted_initial_state_is_set_if_there_is_one", func(t *testing.T) {
		s := newInMemoryStorageWithState(&pb.PersistentState{Term: 10, VotedFor: 4})

		copied := params
		copied.StateStorage = s

		r, err := newRaft(copied)
		require.NoError(t, err)
		require.Equal(t, uint64(10), r.term)
		require.Equal(t, uint64(4), r.votedFor)
	})

	t.Run("passed_params_are_set_correctly", func(t *testing.T) {
		r, err := newRaft(params)
		require.NoError(t, err)
		require.Equal(t, uint64(5), r.id)
		require.Equal(t, RoleFollower, r.role)
		require.Equal(t, None, r.leaderId)
		require.Equal(t, []uint64{1, 2, 3}, r.peers)
		require.Equal(t, 10, r.maxLeaderElectionTimeout)
		require.Equal(t, 3, r.heartBeatTimeout)
		require.NotNil(t, r.logger)
	})
}

func TestPersistentState(t *testing.T) {
	r := &raft{term: 2, votedFor: 5}
	require.Equal(t, pb.PersistentState{Term: 2, VotedFor: 5}, r.persistentState())
}

func TestSoftState(t *testing.T) {
	r := &raft{role: RoleCandidate, leaderId: 4}
	require.Equal(t, SoftState{Lead: 4, Role: RoleCandidate}, r.softState())
}

func createRaft(storage storage.LogStorage) *raft {
	if storage == nil {
		storage = newInMemoryStorage([]pb.Entry{})
	}

	log, err := newRaftLog(storage)
	if err != nil {
		panic(err)
	}

	r := &raft{
		id:                       _localNodeId,
		term:                     5,
		role:                     RoleFollower,
		leaderElectionTimeout:    _testElectionTimeout,
		maxLeaderElectionTimeout: _testElectionTimeout * 2,
		heartBeatTimeout:         _testHeartBeatTimeout,

		// remove non-determinism from tests
		randIntn: func(_ int) int {
			return 0
		},
		raftLog: log,

		logger: logging.NewLogger(fmt.Sprintf("node %d", _localNodeId), logging.Trace),
	}

	return r
}

func drainMessages(r *raft) []pb.Message {
	msg := r.messages
	r.messages = nil
	return msg
}

func allEntries(r *raft) []pb.Entry {
	entries, err := r.raftLog.allEntries()
	if err != nil {
		panic(err)
	}
	return entries
}

func moveInTime(r *raft, ticks int) {
	for i := 0; i < ticks; i++ {
		_ = r.step(pb.Message{Type: pb.MsgTick})
	}
}

func newInMemoryStorage(entries []pb.Entry) *storage.InMemory {
	s := &storage.InMemory{}
	if err := s.Append(entries...); err != nil {
		panic(err)
	}

	return s
}

func newInMemoryStorageWithState(state *pb.PersistentState) *storage.InMemory {
	s := &storage.InMemory{}
	if err := s.SetState(*state); err != nil {
		panic(err)
	}

	return s
}
