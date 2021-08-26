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
	"log"

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
	log := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	Must(err)
	lastIndex, err := storage.LastIndex()
	Must(err)

	log.stabled = lastIndex // lastIndex is the last index of memory.entries
	log.applied = firstIndex - 1
	log.committed = firstIndex - 1
	log.entries, _ = storage.Entries(firstIndex, lastIndex+1) // +1 because [,) left close right open range

	return log
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
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if n := len(l.entries); n != 0 {
		return l.entries[n-1].Index
	}
	if !IsEmptySnap(l.pendingSnapshot) {
		return l.pendingSnapshot.Metadata.Index
	}

	return 0
}

func (l *RaftLog) FirstIndex() uint64 {
	i, _ := l.storage.FirstIndex()
	if i < uint64(len(l.entries)) && len(l.entries) != 0 {
		return l.entries[0].Index
	} else {
		DebugPrintf("WARNING", "Detect empty entry in FirstIndex()")
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// the valid term range is [index of dummy entry, last index]
	dummyIndex := l.FirstIndex() - 1

	if i < dummyIndex || i > l.LastIndex() {
		// TODO: return an error instead?
		return 0, nil
	}

	if i < l.FirstIndex() {
		if !IsEmptySnap(l.pendingSnapshot) {
			return i, nil
		}
	}

	si, err := l.storage.Term(i)
	if err == nil {
		return si, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	Must(err)
	return 0, nil
}

func (l *RaftLog) lastTerm() uint64 {
	term := mustTerm(l.Term(l.LastIndex()))
	return term
}

func (l *RaftLog) isUpToDate(lastidx, logterm uint64) bool {
	lastTerm := l.lastTerm()
	if logterm > lastTerm {
		return true
	} else if logterm == lastTerm && lastidx >= l.LastIndex() {
		return true
	}
	return false
}
