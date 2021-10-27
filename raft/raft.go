// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic("init RaftLog error")
	}
	r := new(Raft)
	r.id = c.ID
	r.Term = hardState.Term
	r.Vote = hardState.Vote
	r.electionTimeout = c.ElectionTick
	r.heartbeatTimeout = c.HeartbeatTick
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range c.peers {
		r.Prs[peer] = new(Progress)
	}
	r.RaftLog = newLog(c.Storage)
	r.RaftLog.committed = hardState.Commit
	r.RaftLog.applied = c.Applied
	return r
}

func (r *Raft) sendRequestVote(to uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(lastIndex)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   lastIndex,
	})
	return true
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var entries []*pb.Entry
	lastIndex := r.RaftLog.LastIndex()
	ents, _ := r.RaftLog.Entries(r.Prs[to].Next, lastIndex+1)
	for _, entry := range ents {
		e := entry
		entries = append(entries, &e)
	}
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgAppend,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  prevLogTerm,
		Index:    prevLogIndex,
		Entries:  entries,
		Commit:   r.RaftLog.committed,
		Snapshot: nil,
		Reject:   false,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	default:
		if r.electionElapsed > r.electionTimeout+2*rand.Intn(r.electionTimeout) {
			r.electionElapsed = 0
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = 0
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	for _, progress := range r.Prs {
		progress.Next = r.RaftLog.LastIndex() + 1
		progress.Match = 0
	}
	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Entries: []*pb.Entry{{}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, 0)
		}
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			} else {
				for peer := range r.Prs {
					if peer != r.id {
						r.sendRequestVote(peer)
					}
				}
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			} else {
				for peer := range r.Prs {
					if peer != r.id {
						r.sendRequestVote(peer)
					}
				}
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVoteResponse:
			if !m.Reject {
				r.votes[m.From] = true
			} else {
				r.votes[m.From] = false
			}
			vote := 0
			for _, v := range r.votes {
				if v {
					vote++
					if vote > len(r.Prs)/2 {
						r.becomeLeader()
					}
				}
			}
			// a quorum of nodes rejected, switch to StateFollower
			if len(r.votes)-vote > len(r.Prs)/2 {
				r.becomeFollower(r.Term, 0)
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			if m.Index < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgBeat:
			for peer := range r.Prs {
				if peer != r.id {
					r.sendHeartbeat(peer)
				}
			}
		case pb.MessageType_MsgPropose:
			index := r.RaftLog.LastIndex() + 1
			for _, entry := range m.Entries {
				entry.Index = index
				entry.Term = r.Term
				index++
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
			r.Prs[r.id].Next = index
			r.Prs[r.id].Match = index - 1
			if len(r.Prs) == 1 {
				r.RaftLog.committed += uint64(len(m.Entries))
			} else {
				for id := range r.Prs {
					if id != r.id {
						r.sendAppend(id)
					}
				}
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			if m.Reject {
				r.Prs[m.From].Next--
				r.sendAppend(m.From)
			} else {
				r.Prs[m.From].Match = m.Index
				r.Prs[m.From].Next = m.Index + 1
				var matchIndexes uint64Slice
				for _, progress := range r.Prs {
					matchIndexes = append(matchIndexes, progress.Match)
				}
				sort.Sort(matchIndexes)
				// a majority of matchIndexes[i] ≥ matchIndex
				matchIndex := matchIndexes[(len(matchIndexes)-1)/2]
				for i := matchIndex; i > r.RaftLog.committed; i-- {
					term, _ := r.RaftLog.Term(i)
					if term == r.Term {
						r.RaftLog.committed = i
						// tell the followers to commit
						_ = r.Step(pb.Message{
							MsgType: pb.MessageType_MsgPropose,
							To:      r.id,
							From:    r.id,
						})
						break
					}
				}
			}
		}
	}
	return nil
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
	}
	if m.Term < r.Term {
		msg.Reject = true
	} else {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, 0)
		}
		lastIndex := r.RaftLog.LastIndex()
		term, _ := r.RaftLog.Term(lastIndex)
		if (r.Vote == 0 || r.Vote == m.From) && (term < m.LogTerm || (term == m.LogTerm && lastIndex <= m.Index)) {
			r.Vote = m.From
		} else {
			msg.Reject = true
		}
	}
	msg.Term = r.Term
	r.msgs = append(r.msgs, msg)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
	}
	if m.Term < r.Term {
		msg.Reject = true
	} else {
		r.becomeFollower(m.Term, m.From)
		term, err := r.RaftLog.Term(m.Index)
		// log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		if err != nil || term != m.LogTerm {
			msg.Reject = true
		} else {
			r.RaftLog.appendEntries(m.Index, m.Entries)
			lastNewEntryIndex := m.Index + uint64(len(m.Entries))
			if m.Commit > r.RaftLog.committed {
				if m.Commit > lastNewEntryIndex {
					r.RaftLog.committed = lastNewEntryIndex
				} else {
					r.RaftLog.committed = m.Commit
				}
			}
		}
	}
	msg.Index = r.RaftLog.LastIndex()
	msg.Term = r.Term
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
	}
	if m.Term < r.Term {
		msg.Reject = true
	} else {
		r.becomeFollower(m.Term, m.From)
		// r.RaftLog.appendEntries(m.Index, m.Entries)
		lastNewEntryIndex := m.Index + uint64(len(m.Entries))
		if m.Commit > r.RaftLog.committed {
			if m.Commit > lastNewEntryIndex {
				r.RaftLog.committed = lastNewEntryIndex
			} else {
				r.RaftLog.committed = m.Commit
			}
		}
	}
	msg.Index = r.RaftLog.LastIndex()
	msg.Term = r.Term
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
