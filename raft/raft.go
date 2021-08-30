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
	"fmt"
	"log"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

// type CampaignType string // nearly useless for now

const (
	StateFollower  StateType = iota //0
	StateCandidate                  // 1
	StateLeader                     // 2
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

	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

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

	//Pengding conf flag
	PendingConf bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	hs, _, err := raftLog.storage.InitialState()
	Must(err)

	peers := c.peers

	r := &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	for _, val := range peers {
		r.Prs[val] = &Progress{Next: raftLog.LastIndex() + 1, Match: raftLog.LastIndex()}
	}

	if !IsEmptyHardState(hs) {
		if hs.Commit < raftLog.committed || hs.Commit > raftLog.LastIndex() {
			log.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, hs.Commit, raftLog.committed, raftLog.LastIndex())
		}
		r.RaftLog.committed = hs.GetCommit()
		r.Term = hs.GetTerm()
		r.Vote = hs.GetVote()
	}
	if c.Applied > 0 {
		if c.Applied == 0 {

		} else if r.RaftLog.committed < c.Applied || c.Applied < r.RaftLog.applied {
			log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", c.Applied, r.RaftLog.applied, r.RaftLog.committed)
		} else {
			r.RaftLog.applied = c.Applied
		}
	}

	r.becomeFollower(r.Term, r.Lead)
	//randomize the timeout to avoid votes split
	nodeList := nodes(r)
	r.setRandomizedElectionTimeout()
	r.heartbeatTimeout = 10 * r.randomizedElectionTimeout // follow the comment instruction
	DebugPrintf("INFO", "newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, nodeList, r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.Term)

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prog := r.Prs[to]
	term, terr := r.RaftLog.Term(prog.Next - 1)
	ents := r.RaftLog.entries[prog.Next:]
	pEnts := make([]*pb.Entry, 0)
	for i := range ents {
		pEnts = append(pEnts, &ents[i])
	}
	msg := pb.Message{}
	// when needed entries are compact
	// we need to send snapshot
	// TODO: send snapshot
	if terr != nil {
		// send snapshot
	} else {
		msg = pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			Index:   prog.Next - 1,
			LogTerm: term,
			Entries: pEnts,
			Commit:  r.RaftLog.committed,
		}
	}
	r.sendMessage(msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

func (r *Raft) sendMessage(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

func (r *Raft) setRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.New(rand.NewSource(time.Now().UnixNano())).Intn(r.electionTimeout)
}

//the number of senate
func (r *Raft) quorum() int {
	senates := len(r.Prs)/2 + 1
	return senates
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = 0
	}

	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.setRandomizedElectionTimeout()

	//abortLeaderTransfer

	r.votes = make(map[uint64]bool)

	// reset 的时候 针对Prs中的每一个pr，我们将progress中的
	// next和match进行重新设定。其中next就是当前raft节点实例
	// 中的raftlog所记录的最后一个memorystorage.ents数组中
	// 最后一个entries的索引，可以理解为已经持久化部分的最后一个
	// 索引。那么Next、Match重新设定的值就显而易见了。
	for i, pr := range r.Prs {
		*pr = Progress{Next: r.RaftLog.LastIndex() + 1}
		if i == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	}

	//TODO: pengdingconfi
}

// is timeout ?
func (r *Raft) isElectionTimeout() bool {
	return r.randomizedElectionTimeout <= r.electionElapsed
}

func (r *Raft) isHeartbeatTimeout() bool {
	return r.heartbeatElapsed >= r.heartbeatTimeout
}

// Logic tick for follwer and candidate
func (r *Raft) electionTick() {
	r.electionElapsed++
	if r.isElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

// HeartbeatIick for leader
func (r *Raft) heartBeatTick() {
	r.heartbeatElapsed++
	if r.isHeartbeatTimeout() {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionTick()
	case StateCandidate:
		r.electionTick()
	case StateLeader:
		r.heartBeatTick()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.reset(term)
	r.Lead = lead
	DebugPrintf("INFO", "%x became a follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic(fmt.Sprintf("%x cannot transfer from leader to candidate.", r.id))
	}
	r.reset(r.Term + 1)
	r.Vote = r.id // 首先投给自己？
	r.State = StateCandidate
	DebugPrintf("INFO", "%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	if r.State == StateLeader {
		panic("Cannot convert follower to leader.")
	}

	r.reset(r.Term) // 其实就是每一次都重新做一次初始化
	r.Lead = r.id
	r.State = StateLeader
	confCnt := 0
	for i := 0; i < int(r.RaftLog.committed)+1; i++ {
		if confCnt > 1 {
			panic("BecomeLeader: multiple uncommitted config entry")
		}
		if r.RaftLog.entries[i].EntryType == pb.EntryType_EntryConfChange {
			confCnt++
		}
	}
	if confCnt == 1 {
		r.PendingConf = true
	}

	// apply note above: add noop
	noopEntry := pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil,
	}
	r.rawAppendEntry(noopEntry)

	//Cancel testing for next\match, assume all success before
	for i, p := range r.Prs {
		if i == r.id {
			p.Next = r.RaftLog.LastIndex() + 1
			p.Match = r.RaftLog.LastIndex()
		} else {
			p.Next = r.RaftLog.LastIndex() + 1
		}
	}
}

func (r *Raft) rawAppendEntry(ent ...pb.Entry) {
	r.RaftLog.entries = append(r.RaftLog.entries, ent...)
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// This block mainly learn from etcd

	// handle term
	switch {
	case m.Term == 0:
		//test doesn't set term for the local messages:
		//MessageType_MsgHup, MessageType_MsgBeat and MessageType_MsgPropose
	case m.Term > r.Term:
		//TODO: handle become follower
		switch {
		default:
			DebugPrintf("INFO", "%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.MsgType, m.From, m.Term)
			//TODO: handle vote msg
			//becomeFollower but continue
			if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}
	case m.Term < r.Term:
		//ah... Distinct different situation is too complicate... ignore all for a while...
		DebugPrintf("INFO", "%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		return nil //After ignore, return.
	}

	//If unnecessary to becomefollower and m.term >= r.term continue Step
	//Suitable for three roles
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			// trigger election
			// if there is unapplied entries, stop.
			// commit before applied
			ents, err := r.RaftLog.storage.Entries(r.RaftLog.applied+1, r.RaftLog.committed+1)
			Must(err)
			//can't elect when there are confs is not up to date.
			if lenEnts := len(ents); lenEnts != 0 && r.RaftLog.committed > r.RaftLog.applied {
				DebugPrintf("WARNING", "%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, lenEnts)
				return nil
			}
			DebugPrintf("INFO", "%x is starting election at term %d", r.id, r.Term)
			if len(r.Prs) == 1 {
				r.becomeLeader()
			} else {
				r.campaign()
			}
		} else {
			DebugPrintf("INFO", "%x received MsgHup but it is leader already", r.id)
		}
	case pb.MessageType_MsgRequestVote:
		if (r.Vote == None || m.Term > r.Term || r.Vote == m.From) && r.RaftLog.isUpToDate(m.GetIndex(), m.GetLogTerm()) {
			// No prevote here
			// r.Vote == None: 还没投票
			// m.Term > r.Term: 更新的term
			// r.Vote == m.From: 已经投过给他了
			// isUptoDate: term更大的更新，或term相等，index更大的更新
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    m.Term, //对方的term
			}
			r.sendMessage(msg)
			r.electionElapsed = 0
			r.Vote = m.From
			DebugPrintf("INFO", "%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
		} else { // 如果不符合上述的判断情况，此时msg中的reject字段就派上用场
			r.Vote = m.From
			DebugPrintf("INFO", "%x [logterm: %d, index: %d, vote: %x] reject %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term, //reject时，是我方的term
				Reject:  true,
			}
			r.sendMessage(msg)
		}
	default:
		switch r.State {
		case StateFollower:
			stepFollower(r, m)
		case StateCandidate:
			stepCandidate(r, m)
		case StateLeader:
			stepLeader(r, m)
		}
	}

	return nil
}

//election
func (r *Raft) campaign() {
	r.becomeCandidate()
	// r.Term = r.Term + 1 term + 1 operated in becomecandidate()
	term := mustTerm(r.RaftLog.Term(r.RaftLog.LastIndex()))

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		DebugPrintf("INFO", "%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.RaftLog.entries[r.RaftLog.LastIndex()], r.RaftLog.LastIndex(), "pb.MessageType_MsgRequestVote", id, r.Term)

		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			LogTerm: term,
			Index:   r.RaftLog.LastIndex(),
		}
		r.sendMessage(msg)
	}
}

//Different roles' step function
func stepLeader(r *Raft, m pb.Message) {

}

func stepCandidate(r *Raft, m pb.Message) {
	//only handle vote response
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		granted := 0
		if _, flag := r.votes[m.From]; !flag {
			r.votes[m.From] = m.Reject
		}
		for _, val := range r.votes {
			if val {
				granted++
			}
		}
		if granted == r.quorum() { // note: ==
			r.becomeLeader()
			r.bcastAppend()
		} else if r.quorum() == len(r.votes)-granted { //note: ==
			r.becomeFollower(r.Term, None) // didn't get enough votes
		}
	}
}

func stepFollower(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.Lead = m.From

	if m.Index < r.RaftLog.committed {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Index:   r.RaftLog.committed,
		}
		r.sendMessage(msg)
		return
	}
	if r.appTermMatch(m.Index, m.LogTerm) {

	} else {

	}
}

func (r *Raft) appTermMatch(index, logTerm uint64) bool {
	tempTerm, err := r.RaftLog.Term(index)
	if err != nil {
		return false
	}
	return tempTerm == logTerm
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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
