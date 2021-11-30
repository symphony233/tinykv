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
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap-incubator/tinykv/log"

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
		if hs.GetCommit() != 0 {
			r.RaftLog.committed = hs.GetCommit()
		}
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
	r.setRandomizedElectionTimeout() // random election timeout
	// r.electionTimeout = r.randomizedElectionTimeout
	// r.heartbeatTimeout = 10 * r.randomizedElectionTimeout // follow the comment instruction
	log.Infof("newRaft %x [peers: [%v], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d], heartbeatTimeout %d, electionTimeout %d",
		r.id, nodeList, r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.lastTerm(), r.heartbeatTimeout, r.electionTimeout)
	// DebugPrintf("INFO", "newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
	// r.id, nodeList, r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.Term)

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prog := r.Prs[to]
	prev_match_term, terr := r.RaftLog.Term(prog.Match)
	ents, _ := r.RaftLog.slice(prog.Next, r.RaftLog.LastIndex()+1)
	log.Warnf("%d with %d's prog Match: %d, Next: %d, raftlog-entries len %d, ents len %d",
		r.id, to, prog.Match, prog.Next, len(r.RaftLog.entries), len(ents))
	pEnts := make([]*pb.Entry, 0)
	for i := range ents {
		pEnts = append(pEnts, &ents[i])
		log.Warnf("sendAppend log: term %d index %d", pEnts[i].Term, pEnts[i].Index)
	}
	msg := pb.Message{}
	// when needed entries are compact
	// we need to send snapshot
	// TODO: send snapshot
	if terr != nil {
		// send snapshot
		log.Infof("terr in sendAppend != nil")
	} else {
		msg = pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			Index:   prog.Match,
			Term:    r.Term,
			From:    r.id,
			To:      to,
			LogTerm: prev_match_term,
			Entries: pEnts,
			Commit:  r.RaftLog.committed,
		}
	}
	log.Infof("%d sendAppend at term %d to %d", r.id, r.Term, to)
	r.sendMessage(msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		From:    r.id,
		To:      to,
	}
	r.sendMessage(msg)
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
		// log.Infof("%d build progress for %d", r.id, i)
		*pr = Progress{Next: r.RaftLog.LastIndex() + 1, Match: r.RaftLog.LastIndex()}
		if i == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	}

	//TODO: pengdingconfig
}

// is electiontimeout ?
func (r *Raft) isElectionTimeout() bool {
	return r.randomizedElectionTimeout <= r.electionElapsed
	// return r.electionTimeout <= r.electionElapsed
}

func (r *Raft) isHeartbeatTimeout() bool {
	return r.heartbeatElapsed >= r.heartbeatTimeout
}

// Logic tick for follwer and candidate
func (r *Raft) electionTick() {
	r.electionElapsed++
	if r.isElectionTimeout() {
		log.Infof("%d triggered electionTimeout at term %d, with timeout baseline %d, now it's %d", r.id, r.Term, r.electionTimeout, r.electionElapsed)
		r.electionElapsed = 0

		//pass MsgHub to start a new election.
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

// HeartbeatTick for leader
func (r *Raft) heartBeatTick() {
	r.heartbeatElapsed++
	if r.isHeartbeatTimeout() {
		log.Infof("%d trigger heartbeat time out as Leader at term %d", r.id, r.Term)
		r.heartbeatElapsed = 0
		//trigger leader to send periodic heartbeat message
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
	log.Infof("%x became a follower at term %d", r.id, r.Term)
	// DebugPrintf("INFO", "%x became a follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic(fmt.Sprintf("%x cannot transfer from leader to candidate.", r.id))
	}
	r.reset(r.Term + 1)
	r.Vote = r.id // 首先投给自己？
	r.votes[r.id] = true
	r.State = StateCandidate
	log.Infof("%x became candidate at term %d", r.id, r.Term)
	// DebugPrintf("INFO", "%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	if r.State == StateFollower && len(r.Prs) != 1 {
		panic("Cannot convert follower to leader.")
	}

	r.reset(r.Term) // 其实就是每一次都重新做一次初始化
	r.Lead = r.id
	r.State = StateLeader
	log.Infof("%d became Leader in term %d", r.id, r.Term)
	confCnt := 0
	for i := 0; i < int(r.RaftLog.committed) && i < len(r.RaftLog.entries); i++ {
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
	// propose_msg := pb.Message{
	// 	MsgType: pb.MessageType_MsgPropose,
	// 	Entries: []*pb.Entry{&noopEntry},
	// 	From:    r.id,
	// 	To:      r.id,
	// 	Term:    r.Term, //hard state term?
	// }
	// r.RaftLog.appendEntry(noopEntry)
	r.rawAppendEntry(noopEntry)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	//Cancel testing for next\match, assume all success before
	//? why???
	// for i, p := range r.Prs {
	// 	if i == r.id {
	// 		p.Next = r.RaftLog.LastIndex()
	// 		p.Match = r.RaftLog.LastIndex() - 1
	// 	} else {
	// 		p.Next = r.RaftLog.LastIndex()
	// 	}
	// }
	// r.sendMessage(propose_msg)
	r.checkCommit()
	r.bcastAppend()
}

func (r *Raft) rawAppendEntry(ent ...pb.Entry) {
	r.RaftLog.entries = append(r.RaftLog.entries, ent...)
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			if !r.sendAppend(id) {
				log.Panicf("In bcastAppend, sendAppend to %d failed", id)
			}
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
			log.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.MsgType, m.From, m.Term)
			// DebugPrintf("INFO", "%x [term: %d] received a %s message with higher term from %x [term: %d]",
			// r.id, r.Term, m.MsgType, m.From, m.Term)
			//TODO: handle vote msg
			//becomeFollower but continue
			//if a candidate or leader discovers that its term is out of data, it reverts to follower immediately.
			if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
				r.becomeFollower(m.GetTerm(), m.From)
			} else {
				r.becomeFollower(m.GetTerm(), None)
			}
		}
		// case m.Term < r.Term:
		// 	//ah... Distinct different situation is too complicate... ignore all for a while...
		// 	log.Warnf("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
		// 		r.id, r.Term, m.MsgType, m.From, m.Term)
		// 	// DebugPrintf("INFO", "%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
		// 	// 	r.id, r.Term, m.MsgType, m.From, m.Term)
		// 	return nil //After ignore, return.
	}

	//If unnecessary to becomefollower and m.term >= r.term continue Step
	//Suitable for three roles
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			// trigger election
			// if there is unapplied entries, stop.
			// commit before applied
			// log.Infof("%d received MsgHup at term: %d, commited: %d", r.id, r.Term, r.RaftLog.committed)
			// ents := r.RaftLog.entries[r.RaftLog.applied+1 : r.RaftLog.committed+1]

			// //can't elect when there are confs is not up to date.
			// if lenEnts := len(ents); lenEnts != 0 && r.RaftLog.committed > r.RaftLog.applied {
			// 	log.Warnf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, lenEnts)
			// 	// DebugPrintf("WARNING", "%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, lenEnts)
			// 	return nil
			// }
			log.Infof("%x is starting election at term %d", r.id, r.Term)
			// DebugPrintf("INFO", "%x is starting election at term %d", r.id, r.Term)
			if len(r.Prs) == 1 { //only one, it's leader.
				r.Term += 1
				r.becomeLeader()
			} else {
				r.campaign()
			}
		} else {
			log.Infof("%x received MsgHup but it is leader already", r.id)
			// DebugPrintf("INFO", "%x received MsgHup but it is leader already", r.id)
		}
	case pb.MessageType_MsgRequestVote:
		if m.GetTerm() >= r.Term && (r.Vote == m.GetFrom() || r.Vote == None) && r.RaftLog.isUpToDate(m.GetIndex(), m.GetLogTerm()) {
			// No prevote here
			// r.Vote == None: 还没投票
			// m.Term > r.Term: 更高的term
			// r.Vote == m.From: 已经投过给他了
			// isUptoDate: 请求方term大，才是uptodate的、合法的

			// log.Infof("%x [logterm: %d, index: %d, vote: %x] accept %s from %x [logterm: %d, index: %d] at term %d",
			// 	r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    m.Term, //对方的term
				Reject:  false,
			}
			if r.State == StateCandidate || r.State == StateLeader {
				r.becomeFollower(m.GetTerm(), None)
			} else {
				r.becomeFollower(m.GetTerm(), None)
			}
			r.sendMessage(msg)
			r.electionElapsed = 0
			r.Vote = m.From
			log.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			// DebugPrintf("INFO", "%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
			// 	r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
		} else { // 如果不符合上述的判断情况，此时msg中的reject字段就派上用场
			// r.Vote = m.From
			log.Infof("%x [logterm: %d, index: %d, vote: %x] reject %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			// DebugPrintf("INFO", "%x [logterm: %d, index: %d, vote: %x] reject %s from %x [logterm: %d, index: %d] at term %d",
			// 	r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term, //reject时，是我方的term
				Reject:  true,
			}
			r.sendMessage(msg)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
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
	// r.Term = r.Term + 1, term + 1 operated in becomecandidate()

	//mustTerm get the lastindex term.
	last_log_term := mustTerm(r.RaftLog.Term(r.RaftLog.LastIndex()))

	//loop to send requestvote in the whole cluster
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		log.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, last_log_term, r.RaftLog.LastIndex(), "pb.MessageType_MsgRequestVote", id, r.Term)
		// DebugPrintf("INFO", "%x [logterm: %d, index: %d] sent %s request to %x at term %d",
		// 	r.id, r.RaftLog.entries[r.RaftLog.LastIndex()], r.RaftLog.LastIndex(), "pb.MessageType_MsgRequestVote", id, r.Term)

		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Commit:  r.RaftLog.committed,
			Term:    r.Term,
			LogTerm: last_log_term,
			Index:   r.RaftLog.LastIndex(),
		}
		r.sendMessage(msg)
	}
}

func (r *Raft) checkCommit() bool {
	quorumSlice := make(uint64Slice, 0)
	// matchMap := make(map[uint64]uint32)
	for _, pr := range r.Prs {
		quorumSlice = append(quorumSlice, pr.Match)
	}

	// for match, cnt := range matchMap {
	// 	if cnt >= uint32(r.quorum()) {
	// 		quorumSlice = append(quorumSlice, match)
	// 	}
	// }
	// log.Infof("quorumSlice: %v", quorumSlice)
	// for i, val := range r.Prs {
	// 	log.Infof("%d prog match: %d", i, val.Match)
	// }
	sort.Sort(sort.Reverse(quorumSlice))
	oldCommitted := r.RaftLog.committed
	tempCommitted := quorumSlice[r.quorum()-1]
	if mustTerm(r.RaftLog.Term(tempCommitted)) == r.Term && (r.RaftLog.committed < tempCommitted || r.RaftLog.committed == 0) {
		r.RaftLog.committed = tempCommitted
		log.Infof("Update committed from %d to %d", oldCommitted, r.RaftLog.committed)
		return true
	}
	log.Infof("Can not update committed, quorumSlice len: %d", len(quorumSlice))
	return false
}

//Different roles' step function
func stepLeader(r *Raft, m pb.Message) {
	prog := r.Prs[m.From]
	if prog == nil {
		log.Warnf("%x no progress available for %x", r.id, m.From)
		// DebugPrintf("INFO", "%x no progress available for %x", r.id, m.From)
		// return
	}

	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		if m.GetReject() {
			//handle reject
			log.Infof("%x received msgApp rejection(lastindex: %d) from %x for sent next index %d",
				r.id, m.GetIndex(), m.From, m.Index)
			// DebugPrintf("INFO", "%x received msgApp rejection(lastindex: %d) from %x for index %d",
			// 	r.id, m.Commit, m.From, m.Index)
			//then retry append
			//modify progress
			// if m.GetIndex() > prog.Match {
			// 	// reach the lower bound directly
			// 	prog.Next = prog.Next - 1
			// 	//send again
			// 	r.sendAppend(m.From)
			// }

			prog.Match = max(prog.Match-1, r.RaftLog.FirstIndex())
			prog.Next = prog.Match + 1
			r.sendAppend(m.GetFrom())
		} else {
			//handle ok
			//?
			log.Infof("%x received msgApp success(lastindex: %d) from %x for sent next index %d, prev match %d",
				r.id, m.GetIndex(), m.From, m.Index, prog.Match)
			if m.GetIndex() > prog.Match {
				prog.Match = m.GetIndex()
			}
			prog.Next = m.GetIndex() + 1
			if r.checkCommit() {
				r.bcastAppend()
			}
		}
	case pb.MessageType_MsgAppend:
		log.Infof("%d received MsgAppend in it's term %d from %d with term %d.", r.id, r.Term, m.GetFrom(), m.GetTerm())
		if m.GetTerm() > r.Term {
			r.becomeFollower(r.Term, m.GetFrom())
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgPropose:
		log.Infof("%d received MsgPropose in term %d", r.id, r.Term)
		// r.handleAppendEntries(m)
		for i, ent := range m.Entries {
			ent.Term = r.Term
			ent.Index = r.RaftLog.LastIndex() + 1 + uint64(i)
			if m.GetTo() == r.id {
				r.Prs[r.id].Match = ent.Index
				r.Prs[r.id].Next = ent.Index + 1
			}
			r.rawAppendEntry(*ent)
		}
		r.checkCommit()
		log.Infof("%d MsgPropose appendEntry at term %d, now last index is %d", r.id, r.Term, r.RaftLog.LastIndex())
		r.bcastAppend()
	case pb.MessageType_MsgBeat:
		for i := range r.Prs {
			if i != r.id {
				r.sendHeartbeat(i)
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:

	}
}

func stepCandidate(r *Raft, m pb.Message) {
	//only handle vote response
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		log.Infof("%d received %v Response from %d to %d at trem %d with reject=%v",
			r.id, "MsgRequestVoteResponse", m.GetFrom(), m.GetTo(), r.Term, m.GetReject())
		granted := 0
		// if _, flag := r.votes[m.From]; !flag {
		r.votes[m.From] = !m.GetReject()
		// }
		for _, val := range r.votes {
			if val {
				granted++
			}
		}
		// log.Infof("granted: %d, needed senates: %d", granted, r.quorum())
		DebugPrintf("INFO", "granted: %d", granted)
		if granted >= r.quorum() { // == ?
			r.becomeLeader()
		} else if len(r.votes)-granted >= r.quorum() { //note: ==
			r.becomeFollower(r.Term, None) // didn't get enough votes
			// pass for now
		}
	case pb.MessageType_MsgAppend:
		//candidate received masappend, indicating there is a validate leader.
		log.Infof("%d received MsgAppend from %d at term %d with previous match logTerm %d, previous Index %d",
			r.id, m.GetFrom(), r.Term, m.GetLogTerm(), m.GetIndex())
		r.becomeFollower(r.Term, m.GetFrom())
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		if r.Term < m.GetTerm() {
			r.becomeFollower(r.Term, m.GetFrom())
			r.RaftLog.committed = m.GetCommit()
			r.sendMessage(m) // send to it's mailbox (as doc.go says)
		}
	}
}

func stepFollower(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		log.Infof("%d received MsgAppend from %d at term %d with previous match logTerm %d, previous Index %d",
			r.id, m.GetFrom(), r.Term, m.GetLogTerm(), m.GetIndex())
		r.handleAppendEntries(m)
	// case pb.MessageType_MsgPropose:
	// 	m.From = r.id
	// 	r.sendMessage(m)
	case pb.MessageType_MsgHeartbeat:
		log.Infof("Follower %d received MsgHeartBeat at term %d, with leader term %d", r.id, r.Term, m.GetTerm())
		// if r.Term < m.GetTerm() {
		// 	r.Lead = m.GetFrom()
		// 	msg := pb.Message{
		// 		MsgType: pb.MessageType_MsgHeartbeatResponse,
		// 		From:    r.id,
		// 		To:      r.Lead,
		// 	}
		// 	r.sendMessage(msg)
		// }
		r.handleHeartbeat(m)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	log.Infof("%d handleAppendEntries at term %d", r.id, r.Term)
	if m.GetTerm() < r.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  true,
			To:      m.GetFrom(),
		}
		r.sendMessage(msg)
	}
	r.electionElapsed = 0
	r.Lead = m.From

	// if m.GetLogTerm() < r.RaftLog.committed {
	// 	msg := pb.Message{
	// 		MsgType: pb.MessageType_MsgAppendResponse,
	// 		To:      m.From,
	// 		From:    r.id,
	// 		Index:   r.RaftLog.committed,
	// 		Reject:  true,
	// 		LogTerm: mustTerm(r.RaftLog.Term(r.RaftLog.committed)),
	// 	}
	// 	r.sendMessage(msg)
	// 	return
	// }
	// term match
	if r.appTermMatch(m.GetIndex(), m.GetLogTerm()) {
		log.Infof("%d received AppendEntries from %d at term %d, and term match",
			r.id, m.GetFrom(), r.Term)
		// lastIdx := m.Index + uint64(len(m.Entries))

		confictIdx := -1
		cleanEntry := make([]*pb.Entry, 0)
		//find conflict
		for _, ent := range m.Entries {
			if !r.appTermMatch(ent.Index, ent.Term) {
				existTerm, err := r.RaftLog.Term(ent.Index)
				Must(err)
				if ent.Index <= r.RaftLog.LastIndex() {
					log.Warnf("found conflict at index %d [existing term: %d, conflicting term: %d]", ent.Index,
						existTerm, ent.Term)
					// DebugPrintf("INFO", "found conflict at index %d [existing term: %d, conflicting term: %d]", ent.Index,
					// 	existTerm, ent.Term)
					confictIdx = int(ent.Index)
					break
				}
			}
		}

		if confictIdx == -1 {
			confictIdx = 0
			// delete existed entries
			if len(m.Entries) == 0 || m.Entries[0].GetIndex() <= r.RaftLog.LastIndex() {
				m.Entries = cleanEntry
			}
		}
		//different conflict situation
		if confictIdx == 0 {
			ents := make([]pb.Entry, 0)
			for _, ent := range m.Entries {
				ents = append(ents, *ent)
			}
			r.RaftLog.appendEntry(ents...)
			log.Infof("%d append entry with last logTerm %d andlastIndex %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex())
		} else if confictIdx <= int(r.RaftLog.committed) && confictIdx >= 0 {
			//impossible situation
			panic(fmt.Sprintf("entry %d conflict with committed entry [committed(%d)]", confictIdx, r.RaftLog.committed))
		} else {
			//truncate in unmatch point
			r.RaftLog.entries, _ = r.RaftLog.slice(r.RaftLog.FirstIndex(), uint64(confictIdx)) //right open [,)
			ents := make([]pb.Entry, 0)
			for _, ent := range m.Entries {
				ents = append(ents, *ent)
			}
			// r.rawAppendEntry(ents...)
			r.RaftLog.appendEntry(ents...)
			log.Infof("After conflict truncate append, now the lastIndex is: %d, l.stabled is: %d", r.RaftLog.LastIndex(), r.RaftLog.stabled)
		}
		if len(m.Entries) != 0 {
			r.RaftLog.stabled = min(r.RaftLog.stabled, m.Entries[0].GetIndex())
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Index:   r.RaftLog.LastIndex(),
			Reject:  false,
			Term:    r.Term,
		}
		// r.Term = m.GetTerm()
		r.sendMessage(msg)
	} else {
		mIdx, err := r.RaftLog.Term(m.GetIndex())
		Must(err)
		log.Infof("%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
			r.id, mIdx, m.Index, m.LogTerm, m.Index, m.From)
		// DebugPrintf("INFO", "%x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
		// 	r.id, mIdx, m.Index, m.LogTerm, m.Index, m.From)
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  true,
			Commit:  r.RaftLog.LastIndex(), // trick, use index field as reject hint
			Index:   m.Index,
			To:      m.GetFrom(),
			From:    r.id,
			Term:    r.Term,
			LogTerm: mustTerm(r.RaftLog.Term(r.RaftLog.LastIndex())), // use lastTerm?
		}
		r.sendMessage(msg)
	}

	r.RaftLog.committed = max(r.RaftLog.committed, min(m.GetCommit(), r.RaftLog.LastIndex()))

}

func (r *Raft) appTermMatch(index, logTerm uint64) bool {
	tempTerm, err := r.RaftLog.Term(index)
	if err != nil {
		panic(err)
	}
	// if tempTerm == 0 && len(r.RaftLog.entries) != 0 {
	// 	return false
	// }
	return tempTerm == logTerm || tempTerm == 0
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.committed = max(r.RaftLog.committed, min(m.GetCommit(), r.RaftLog.LastIndex()))
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.GetFrom(),
		From:    r.id,
	}
	r.sendMessage(msg)
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
