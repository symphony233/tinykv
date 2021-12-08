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
	"fmt"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage can not be nil")
	}
	r_log := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	Must(err)
	lastIndex, err := storage.LastIndex()
	Must(err)

	r_log.stabled = lastIndex // lastIndex is the last index of memory.entries
	r_log.applied = firstIndex - 1
	r_log.committed = firstIndex - 1
	r_log.entries, _ = storage.Entries(firstIndex, lastIndex+1) // +1 because [,) left close right open range

	return r_log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return make([]pb.Entry, 0)
	}
	log.Infof("Get UnstableEntries with entries' length: %d and l.stabled: %d", len(l.entries), l.stabled)
	return l.entries[l.stabled-l.entries[0].Index+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// i := l.committed
	// j := l.applied + 1
	log.Infof("In nextEnts() begin with appliedEntry: %d, committedEntry: %d", l.applied, l.committed)
	// entslice := make([]pb.Entry, 0)
	// // ltoa(l)
	// if len(l.entries) != 0 {
	// 	for j <= i {
	// 		entslice = append(entslice, l.entries[j-l.entries[0].Index])
	// 		j++
	// 	}
	// }
	// return entslice
	begin := max(l.applied+1, l.FirstIndex())
	if l.committed < begin {
		return nil
	}
	ents, err := l.slice(begin, l.committed+1) //[,)
	if err != nil {
		panic(err)
	}
	return ents
}

// applied
func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if i > l.committed || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

// stable entries
func (l *RaftLog) stableTo(index, term uint64) {
	if index == 0 {
		return
	}
	idxTerm, err := l.Term(index)
	if err != nil {
		return
	}
	if idxTerm == term && index >= l.stabled {
		l.stabled = index
	}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var idx uint64 = 0

	if !IsEmptySnap(l.pendingSnapshot) {
		idx = l.pendingSnapshot.Metadata.Index
	}
	if n := len(l.entries); n != 0 {
		return max(idx, l.entries[len(l.entries)-1].Index)
	}
	st_idx, err := l.storage.LastIndex()
	Must(err)

	return max(st_idx, idx)
}

//from etcd
// hasNextEnts returns if there is any available entries for execution. This
// is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
func (l *RaftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.FirstIndex())
	return l.committed+1 > off
}

func (l *RaftLog) FirstIndex() uint64 {
	i, _ := l.storage.FirstIndex()
	// if err != nil {
	// 	// log.Warning("Detect empty entry in FirstIndex()")
	// 	return 0
	// }

	if len(l.entries) != 0 && l.entries[0].Index < i {
		return l.entries[0].Index
	} else {
		// log.Warning("Detect empty entry in FirstIndex()")
		return i
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// the valid term range is [index of dummy entry, last index]
	// var preIndex uint64 = 0
	// if len(l.entries) != 0 {
	// 	preIndex = l.entries[0].Index
	// }

	dummyIndex := l.FirstIndex() - 1
	// log.Infof("Want to get Term for Index [%d], with entries[0].Index [%d], dummyIndex [%d]", i, preIndex, dummyIndex)
	if i < dummyIndex || i > l.LastIndex() || i == 0 {
		// TODO: return an error instead?
		fmt.Errorf("require term with the index %d out of range, dummyIndex %d lastIndex %d", i, dummyIndex, l.LastIndex())
		return 0, nil
	}

	if len(l.entries) != 0 {
		tempIndex := int(i) - int(l.entries[0].Index)
		if tempIndex >= 0 && tempIndex <= int(len(l.entries)-1) {
			return l.entries[tempIndex].Term, nil
		}
	}
	//storage.term retunr the term of entry i between firstIndex and lastIndex
	si, err := l.storage.Term(i)
	if err == nil {
		return si, nil
	} else {
		if err == ErrCompacted || err == ErrUnavailable {
			if !IsEmptySnap(l.pendingSnapshot) {
				if i == l.pendingSnapshot.Metadata.Index {
					return l.pendingSnapshot.Metadata.Term, nil
				} else if i < l.pendingSnapshot.Metadata.Index {
					err = ErrCompacted
				}
			}
			return 0, err
		}
	}
	panic(err)
	// if err == ErrCompacted || err == ErrUnavailable {
	// 	return 0, err
	// }

	// return 0, nil
}

func (l *RaftLog) appendEntry(entries ...pb.Entry) uint64 {
	if len(entries) == 0 {
		return l.LastIndex()
	}
	if entries[0].Index-1 < l.committed {
		panic(fmt.Sprintf("after(%d) is out of range [committed(%d)]", entries[0].Index-1, l.committed))
	}
	after := entries[0].Index
	switch {
	case after == l.LastIndex()+1 || len(l.entries) == 0:
		l.entries = append(l.entries, entries...)
	case after <= l.entries[0].GetIndex():
		l.stabled = after - 1
		l.entries = entries
		// tempEntries := l.entries[0 : l.stabled+1]
		// l.entries = make([]pb.Entry, l.stabled+1)
		// copy(l.entries, tempEntries)
		// l.entries = append(l.entries, entries...)
	default:
		ents, _ := l.slice(l.entries[0].GetIndex(), after)
		l.entries = append(ents, entries...)
	}
	l.stabled = min(l.stabled, after-1)
	log.Infof("Now appendEntry with lastIndex %d, lastTerm %d", l.LastIndex(), l.entries[len(entries)-1].Term)
	return l.LastIndex()
}

func (l *RaftLog) lastTerm() uint64 {
	term := mustTerm(l.Term(l.LastIndex()))
	return term
	// term, _ := l.Term(l.LastIndex())
	// return term
}

func (l *RaftLog) isUpToDate(lastidx, logterm uint64) bool {
	lastTerm := l.lastTerm()
	if logterm > lastTerm { //as description in paper, >= in term is uptodate.
		return true
	} else if logterm == lastTerm && lastidx >= l.LastIndex() {
		return true
	}
	return false
}

func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo == uint64(1) && hi == uint64(0) || lo >= hi {
		return nil, nil
	}
	var ents = make([]pb.Entry, 0)
	if len(l.entries) != 0 {
		if hi > l.entries[0].GetIndex() {
			ents = l.entries[max(lo, l.entries[0].Index)-l.entries[0].Index : hi-l.entries[0].Index]
		}
		var storageEnts = make([]pb.Entry, 0)
		var err error
		switch {
		case len(ents) == 0:
			storageEnts, err = l.storage.Entries(lo, min(hi, l.LastIndex()+1))
		case ents[0].GetIndex() > lo:
			storageEnts, err = l.storage.Entries(lo, ents[0].GetIndex())
		}

		switch err {
		case ErrCompacted:
			return nil, err
		case ErrUnavailable:
			panic("entries is unavailable in storage")
		case nil:
			break
		default:
			panic(err)
		}
		ents = append(storageEnts, ents...)
	} else {
		storageEnts, err := l.storage.Entries(lo, min(hi, l.LastIndex()+1))
		if err != nil {
			panic(err)
		}
		copy(ents, storageEnts)
	}
	return ents, nil
}
