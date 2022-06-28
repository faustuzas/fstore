package raft

import (
	"fmt"
	"math/rand"

	"github.com/faustuzas/distributed-kv/logging"
	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/faustuzas/distributed-kv/raft/storage"
	"github.com/faustuzas/distributed-kv/util"
)

var (
	// ErrNotLeader is returned when current node is asked to do operations permitted only by the leader.
	// Clients should resolve fresh leader location and contact it if they receive this error.
	ErrNotLeader = fmt.Errorf("node is not the leader")
)

const (
	// None indicates the absence of the node identifier
	None uint64 = 0
)

// Role describes the role of the raft node
type Role int

const (
	RoleLeader Role = iota + 1
	RoleCandidate
	RoleFollower
)

// electionStatus describes the state of the election
type electionStatus int

const (
	electionPending electionStatus = iota + 1
	electionLost
	electionWon
)

// Params are the parameters required to initialise new raft instance
type Params struct {
	// ID is the unique identifier of the raft instance
	ID uint64

	// StateStorage should provide a way to access last persisted state of the node
	StateStorage storage.StateStorage

	// LogStorage should provide a way to access already stable entries
	LogStorage storage.LogStorage

	// MaxLeaderElectionTimeout defines the maximum timeout for leader election in ticks. The real timeout
	// will be randomly calculated from range [MaxLeaderElectionTimeout / 2, MaxLeaderElectionTimeout]
	MaxLeaderElectionTimeout int

	// HeartBeatTimeout defines how often the leader should send heartbeats. It has to be several times smaller than
	// MaxLeaderElectionTimeout to not cause spurious leader elections
	HeartBeatTimeout int

	// Peers is the IDs of the cluster members. The id mapping to physical connections has to be maintained
	// by other components
	Peers []uint64

	Logger logging.Logger
}

// validate checks whether all Params are complying to the constraints
func (p Params) validate() error {
	if p.ID == 0 {
		return fmt.Errorf("raft ID has to be a greater than 0")
	}

	if p.StateStorage == nil {
		return fmt.Errorf("state storage cannot be nil")
	}

	if p.LogStorage == nil {
		return fmt.Errorf("log storage cannot be nil")
	}

	if p.HeartBeatTimeout <= 0 {
		return fmt.Errorf("heart beat timeout has to be a positive number greater than 0")
	}

	if p.MaxLeaderElectionTimeout <= 0 {
		return fmt.Errorf("max leader election timeout has to be a positive number greater than 0")
	}

	if p.HeartBeatTimeout >= p.MaxLeaderElectionTimeout {
		return fmt.Errorf("max leader election timeout has to be greater than heart beat timeout")
	}

	return nil
}

// raft is the core state machine implementing raft algorithm
type raft struct {
	// id is the unique identifier of the raft instance
	id uint64

	// role is the current role of this raft instance
	role Role

	// leaderId holds the known leader id for the term or 0 if the leader is not known
	leaderId uint64

	// term holds the current term of this raft instance
	term uint64

	// tickFn is invoked regularly and is meant to do raft chores like monitoring leader health, etc.
	tickFn func() error

	// stepFn is Role specific message processing function
	stepFn func(message pb.Message) error

	// ticksFromLeaderAction tracks how many ticks ago we last heard from the leader or last election occurred
	ticksFromLeaderAction int

	// leaderElectionTimeout holds max ticks before we declare the leader as dead and start leader election
	leaderElectionTimeout int

	// maxLeaderElectionTimeout specifies what could be the max leader timeout in ticks + 1
	maxLeaderElectionTimeout int

	// ticksFromHeartBeat tracks how many ticks ago was the last heartbeat
	ticksFromHeartBeat int

	// heartBeatTimeout specifies how many ticks have to pass to send heartbeat messages
	heartBeatTimeout int

	// receivedVotes counts how many votes did candidate received in the current election
	receivedVotes int

	// votedFor holds the server id that we voted for in this term
	votedFor uint64

	// messages holds the buffer of ready to send messages
	messages []pb.Message

	// peers is the active list of cluster members
	peers []uint64

	// randIntn is a function which returns a random int number with a ceiling. Can be overridden in tests
	// to remove non-determinism. By default, uses rand.Intn implementation
	randIntn func(int) int

	// raftLog provides a unified interface to access and manage both stable and unstable log entries
	raftLog *raftLog

	// votes tracks which nodes voted in the election
	votes map[uint64]bool

	// nextIndex tracks next log entry which should be sent to a node
	nextIndex map[uint64]uint64

	// matchIndex tracks maximum log entry known to be replicated to a node
	matchIndex map[uint64]uint64

	logger logging.Logger
}

// newRaft creates a new instance of raft from the provided RaftParams
func newRaft(params Params) (*raft, error) {
	if err := params.validate(); err != nil {
		return nil, fmt.Errorf("validating raft params: %w", err)
	}

	initialState := pb.PersistentState{Term: 1, VotedFor: None}
	state, ok, err := params.StateStorage.State()
	if err != nil {
		return nil, fmt.Errorf("getting persisted state: %w", err)
	}

	if ok {
		initialState = state
	}

	log, err := newRaftLog(params.LogStorage)
	if err != nil {
		return nil, fmt.Errorf("creating raft log: %w", err)
	}

	r := &raft{
		id:                       params.ID,
		role:                     RoleFollower,
		leaderId:                 None,
		term:                     initialState.Term,
		votedFor:                 initialState.VotedFor,
		peers:                    params.Peers,
		maxLeaderElectionTimeout: params.MaxLeaderElectionTimeout,
		heartBeatTimeout:         params.HeartBeatTimeout,
		raftLog:                  log,
		logger:                   params.Logger,
	}

	if err = r.becomeFollower(initialState.Term, None); err != nil {
		return nil, fmt.Errorf("setting state for follower: %w", err)
	}

	return r, nil
}

// step is an entry point to raft instance state mutation. Every message will be processed depending on the
// current role of the raft instance and other rules
func (r *raft) step(message pb.Message) error {
	// check whether we are up-to-date with a cluster and do not need to step down to a follower
	switch {
	case message.Term == 0:
		// message is local, nothing to do...
	case message.Term > r.term:
		// since we got a message with a higher term, become a follower because
		// we have been missing out some action happening in the cluster
		switch message.Type {
		case pb.MsgVote:
			// since the term just started and the voting is ongoing, there is no known leader yet
			if err := r.becomeFollower(message.Term, None); err != nil {
				return fmt.Errorf("becoming follower on vote request: %w", err)
			}
		default:
			if err := r.becomeFollower(message.Term, message.From); err != nil {
				return fmt.Errorf("becoming follower on higher term request: %w", err)
			}
		}
	case message.Term < r.term:
		// discard old messages...
		return nil
	}

	switch message.Type {
	case pb.MsgTick:
		if err := r.tickFn(); err != nil {
			return fmt.Errorf("on tick: %w", err)
		}
	case pb.MsgVote:
		isAllowedToVote, err := r.raftLog.isAllowedToVote(message.LogTerm, message.LogIndex)
		if err != nil {
			return fmt.Errorf("checking if allowed to vote: %w", err)
		}

		canVote := isAllowedToVote && (r.votedFor == 0 || r.votedFor == message.From)
		if !canVote {
			lastTerm, _ := r.raftLog.lastTerm()
			lastIndex, _ := r.raftLog.lastIndex()
			r.logger.Infof("Rejecting vote request (%+v) from node %v. VotedFor - %v, last term - %v, last index - %v", message, message.From, r.votedFor, lastTerm, lastIndex)
			r.send(pb.Message{Type: pb.MsgVoteRes, To: message.From, Reject: true})
			return nil
		}

		r.logger.Infof("granting vote for node #%v at term %d", message.From, message.Term)
		r.votedFor = message.From
		r.ticksFromLeaderAction = 0

		r.send(pb.Message{Type: pb.MsgVoteRes, To: message.From, Reject: false})
	default:
		return r.stepFn(message)
	}

	return nil
}

// becomeFollower changes the raft node state to follower and sets required fields
func (r *raft) becomeFollower(term uint64, leaderId uint64) error {
	r.role = RoleFollower
	r.leaderId = leaderId

	if err := r.reset(term); err != nil {
		return fmt.Errorf("resetting follower state: %w", err)
	}

	r.tickFn = r.leaderElectionTimeoutTickFn
	r.stepFn = r.stepFollower

	return nil
}

// becomeCandidate changes the raft node state to candidate and sets required fields
func (r *raft) becomeCandidate() error {
	if r.role == RoleLeader {
		panic("becoming a candidate from leader state")
	}

	r.role = RoleCandidate
	r.leaderId = None
	r.votes = map[uint64]bool{}

	if err := r.reset(r.term + 1); err != nil {
		return fmt.Errorf("resetting candidate state: %w", err)
	}

	r.tickFn = r.leaderElectionTimeoutTickFn
	r.stepFn = r.stepCandidate

	return nil
}

// becomeLeader changes the raft node state to leader, sets required fields,
// appends first dummy entry to the log and broadcasts it
func (r *raft) becomeLeader() error {
	if r.role == RoleFollower {
		panic("becoming a leader from follower state")
	}

	r.role = RoleLeader
	r.leaderId = r.id

	if err := r.reset(r.term); err != nil {
		return fmt.Errorf("resetting leader state: %w", err)
	}

	r.tickFn = r.leaderHeartBeatTickFn
	r.stepFn = r.stepLeader

	// append empty marker, so we would be able to commit entries from last terms (paper 5.4.2)
	if err := r.appendEntries(pb.Entry{}); err != nil {
		return fmt.Errorf("appending leader marker entry: %w", err)
	}

	if err := r.broadcastAppend(); err != nil {
		return fmt.Errorf("broadcasting append after won election: %w", err)
	}

	return nil
}

// stepFollower is the step function for the follower
func (r *raft) stepFollower(message pb.Message) error {
	switch message.Type {
	case pb.MsgApp:
		r.ticksFromLeaderAction = 0
		if err := r.handleAppendEntriesMessage(message); err != nil {
			return fmt.Errorf("handling append entries message: %w", err)
		}
	case pb.MsgPropose:
		// TODO: implement forwarding
		return ErrNotLeader
	case pb.MsgCampaign:
		if err := r.startCampaign(); err != nil {
			return fmt.Errorf("starting campaign: %w", err)
		}
		return nil
	default:
		r.logger.Warnf("follower received unhandled message with type %s", message.Type.String())
	}
	return nil
}

// stepCandidate is the step function for the candidate
func (r *raft) stepCandidate(message pb.Message) error {
	switch message.Type {
	case pb.MsgVoteRes:
		// record the vote
		r.votes[message.From] = !message.Reject
		if r.votes[message.From] {
			r.logger.Infof("received a vote from node #%d", message.From)
		} else {
			r.logger.Infof("node #%d rejected vote request", message.From)
		}

		switch r.countVotes() {
		case electionWon:
			r.logger.Infof("node %d won election at term %d", r.id, r.term)
			if err := r.becomeLeader(); err != nil {
				return fmt.Errorf("becoming a leader on won election: %w", err)
			}
		case electionLost:
			r.logger.Infof("lost the election, reverting to follower")

			// nodes rejected our candidacy in this term, but we are not sure yet
			// who is the right leader
			if err := r.becomeFollower(r.term, None); err != nil {
				return fmt.Errorf("becoming follower on lost ellection: %w", err)
			}
		}
	case pb.MsgApp:
		if err := r.becomeFollower(message.Term, message.From); err != nil {
			return fmt.Errorf("becoming follower on append: %w", err)
		}

		if err := r.handleAppendEntriesMessage(message); err != nil {
			return fmt.Errorf("handling append messages: %w", err)
		}
	case pb.MsgCampaign:
		if err := r.startCampaign(); err != nil {
			return fmt.Errorf("starting campaing: %w", err)
		}
	case pb.MsgPropose:
		return ErrNotLeader
	default:
		r.logger.Warnf("candidate received unhandled message with type %s", message.Type.String())
	}
	return nil
}

// stepLeader is the step function for the leader
func (r *raft) stepLeader(message pb.Message) error {
	switch message.Type {
	case pb.MsgAppRes:
		if message.Reject {
			// add a safeguard to not go below zero in the case of duplicate rejects
			r.nextIndex[message.From] = util.MaxUint64(r.nextIndex[message.From]-1, 1)
			if err := r.sendAppend(message.From); err != nil {
				return fmt.Errorf("sending append after append rejection: %w", err)
			}
		} else {
			r.nextIndex[message.From] = util.MaxUint64(r.nextIndex[message.From], message.LogIndex+1)
			r.matchIndex[message.From] = util.MaxUint64(r.matchIndex[message.From], message.LogIndex)

			committed, err := r.maybeCommit()
			if err != nil {
				return fmt.Errorf("trying to commit: %w", err)
			}

			if committed {
				if err = r.broadcastAppend(); err != nil {
					return fmt.Errorf("broadcasting append after succesful commit: %w", err)
				}
			}
		}
	case pb.MsgPropose:
		if err := r.appendEntries(message.Entries...); err != nil {
			return fmt.Errorf("appending entries on propose: %w", err)
		}

		if err := r.broadcastAppend(); err != nil {
			return fmt.Errorf("broadcasting append after proposal: %w", err)
		}
	default:
		r.logger.Warnf("leader received unhandled message with type %s", message.Type.String())
	}
	return nil
}

// leaderHeartBeatTickFn is a tick function which checks whether new round of heartbeats is required
func (r *raft) leaderHeartBeatTickFn() error {
	// TODO: currently we are holding one heartbeat timer for all peers, however, communication
	//  with peers can be asymmetrical, therefore, not required heartbeats at the same pace
	r.ticksFromHeartBeat++

	// TODO: an optimisation could be to send heartbeats only to peers that we did not communicate recently
	if r.ticksFromHeartBeat >= r.heartBeatTimeout {

		// TODO: since we are sending appends on basically any proposal, there is no need
		//  to replicate the log again on heartbeats. etcd has an optimisation that they are sending
		//  only empty heartbeat messages and not fully-fledged appends. They are checking whether the peer
		//  needs the append on heartbeat response
		if err := r.broadcastAppend(); err != nil {
			return fmt.Errorf("broadcasting appends on heartbeat: %w", err)
		}

		r.ticksFromHeartBeat = 0
	}

	return nil
}

// leaderElectionTimeoutTickFn tick function which checks whether we need to start
func (r *raft) leaderElectionTimeoutTickFn() error {
	r.ticksFromLeaderAction++

	if r.ticksFromLeaderAction >= r.leaderElectionTimeout {
		r.logger.Warnf("election timeout %v has passed", r.leaderElectionTimeout)

		if err := r.step(pb.Message{Type: pb.MsgCampaign}); err != nil {
			r.logger.Warnf("error while campaigning: %v", err)
		}
	}

	return nil
}

// startCampaign promotes the node in to the candidate and starts the election
func (r *raft) startCampaign() error {
	if err := r.becomeCandidate(); err != nil {
		return fmt.Errorf("becoming candidate: %w", err)
	}

	// vote for yourself first
	r.votedFor = r.id
	r.votes[r.id] = true

	// the cluster consists of only one node, so it immediately becomes the leader
	if r.countVotes() == electionWon {
		if err := r.becomeLeader(); err != nil {
			return fmt.Errorf("becoming a single node cluster leader: %w", err)
		}
		r.logger.Infof("becoming a leader for single node cluster at term %v", r.term)
		return nil
	}

	lastIndex, err := r.raftLog.lastIndex()
	if err != nil {
		return fmt.Errorf("getting last index: %w", err)
	}

	lastTerm, err := r.raftLog.lastTerm()
	if err != nil {
		return fmt.Errorf("getting last term: %w", err)
	}

	// send the votes to all active peers
	for _, peer := range r.peers {
		r.send(pb.Message{
			Type:     pb.MsgVote,
			To:       peer,
			LogIndex: lastIndex,
			LogTerm:  lastTerm,
		})
	}

	return nil
}

// handleAppendEntriesMessage processes the AppendEntries request message
func (r *raft) handleAppendEntriesMessage(message pb.Message) error {
	lastIndex, ok, err := r.raftLog.tryToAppend(message.LogTerm, message.LogIndex, message.Entries...)
	if err != nil {
		return fmt.Errorf("trying to append: %w", err)
	}

	if ok {
		r.raftLog.committedTo(util.MinUint64(message.CommitIndex, lastIndex))
		r.send(pb.Message{Type: pb.MsgAppRes, To: message.From, LogIndex: lastIndex, Reject: false})
	} else {
		r.send(pb.Message{Type: pb.MsgAppRes, To: message.From, Reject: true})
	}

	return nil
}

// countVotes returns the current electionStatus
func (r *raft) countVotes() electionStatus {
	nodesCount := len(r.peers) + 1
	requiredVotes := (nodesCount / 2) + 1

	votesReceived := 0
	for _, granted := range r.votes {
		if granted {
			votesReceived++
		}
	}

	if votesReceived >= requiredVotes {
		return electionWon
	}

	// there is still a chance to win
	if (nodesCount-len(r.votes))+votesReceived >= requiredVotes {
		return electionPending
	}

	return electionLost
}

// reset common properties of the raft instance
func (r *raft) reset(term uint64) error {
	// if term changed just now, node did not for anyone yet
	if term != r.term {
		r.term = term
		r.votedFor = None
	}

	r.ticksFromLeaderAction = 0
	r.receivedVotes = 0
	r.resetElectionTimeout()

	r.nextIndex = map[uint64]uint64{}
	r.matchIndex = map[uint64]uint64{}

	lastIndex, err := r.raftLog.lastIndex()
	if err != nil {
		return fmt.Errorf("getting last index: %w", err)
	}

	for _, peer := range r.peers {
		r.nextIndex[peer] = lastIndex + 1
		r.matchIndex[peer] = 0
	}

	return nil
}

// resetElectionTimeout generates and sets new election timeout
func (r *raft) resetElectionTimeout() {
	fn := r.randIntn
	if fn == nil {
		fn = rand.Intn
	}

	floor := r.maxLeaderElectionTimeout / 2
	r.leaderElectionTimeout = floor + fn(floor)
}

// send appends a message to a soon-to-be-sent messages list
func (r *raft) send(message pb.Message) {
	message.From = r.id
	message.Term = r.term
	r.messages = append(r.messages, message)
}

// appendEntries sets the correct raft index for the entries and appends them to the log
func (r *raft) appendEntries(entries ...pb.Entry) error {
	lastIdx, err := r.raftLog.lastIndex()
	if err != nil {
		return fmt.Errorf("getting last index: %w", err)
	}

	for i := range entries {
		entries[i].Term = r.term
		entries[i].Index = lastIdx + uint64(i) + 1
	}

	if _, err := r.raftLog.appendEntries(entries...); err != nil {
		return fmt.Errorf("appending entries: %w", err)
	}

	// if the cluster consists only of a single node, then we will be able to commit immediately
	if _, err = r.maybeCommit(); err != nil {
		return fmt.Errorf("trying to commit: %w", err)
	}

	return nil
}

// broadcastAppend sends append requests to all current peers
func (r *raft) broadcastAppend() error {
	for _, peer := range r.peers {
		if err := r.sendAppend(peer); err != nil {
			return fmt.Errorf("sending append to %d: %w", peer, err)
		}
	}

	return nil
}

// sendAppend sends an append request to the specified peer
func (r *raft) sendAppend(to uint64) error {
	term, err := r.raftLog.term(r.nextIndex[to] - 1)
	if err != nil {
		return fmt.Errorf("getting term for entry %d: %w", r.nextIndex[to]-1, err)
	}

	entries, err := r.raftLog.entriesFrom(r.nextIndex[to])
	if err != nil {
		return fmt.Errorf("getting entries from %d: %w", r.nextIndex[to], err)
	}

	r.send(pb.Message{
		Type:        pb.MsgApp,
		To:          to,
		LogIndex:    r.nextIndex[to] - 1,
		LogTerm:     term,
		Entries:     entries,
		CommitIndex: r.raftLog.commitIndex,
	})

	return nil
}

// maybeCommit checks whether is possible to move commit index under the current conditions and if
// it is possible moves it. The method returns whether the commit index was moved
func (r *raft) maybeCommit() (bool, error) {
	quorumSize := ((1 + len(r.peers)) / 2) + 1

	lastIdx, err := r.raftLog.lastIndex()
	if err != nil {
		return false, fmt.Errorf("getting last index: %w", err)
	}

	newCommitIndex := r.raftLog.commitIndex
	for entryIdx := r.raftLog.commitIndex + 1; entryIdx <= lastIdx; entryIdx++ {

		// according to paper, leader is not allowed to commit entries not from its own term
		term, err := r.raftLog.term(entryIdx)
		if err != nil {
			return false, fmt.Errorf("getting term for entry %d: %w", entryIdx, err)
		}

		if term != r.term {
			continue
		}

		// assert that locally on leader the entry is always replicated
		// TODO: change when optimising parallel commit and message send
		replicatedTo := 1

		for _, matchIdx := range r.matchIndex {
			if entryIdx <= matchIdx {
				replicatedTo++
			}
		}

		if replicatedTo >= quorumSize {
			newCommitIndex = entryIdx
		} else {
			// any future entries will definitely be not replicated to the quorum
			break
		}
	}

	if r.raftLog.commitIndex == newCommitIndex {
		return false, nil
	}

	r.raftLog.committedTo(newCommitIndex)
	return true, nil
}

// persistentState collects and returns the current state that has to be persisted before sending any messages
func (r *raft) persistentState() pb.PersistentState {
	return pb.PersistentState{
		Term:     r.term,
		VotedFor: r.votedFor,
	}
}

// softState collects and returns the current soft state
func (r *raft) softState() SoftState {
	return SoftState{
		Lead: r.leaderId,
		Role: r.role,
	}
}
