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
	// first is the first index in the entries
	first uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	var err error
	l := new(RaftLog)
	l.first = 1
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic("init RaftLog error")
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic("init RaftLog error")
	}
	if lastIndex >= firstIndex {
		ents, err := storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic("init RaftLog error")
		}
		l.first = firstIndex
		l.entries = append(l.entries, ents...)
		l.stabled = lastIndex
	}
	log.Debugf("new RaftLog is %+v", l)
	return l
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
	if len(l.entries) == 0 || l.stabled == l.LastIndex() {
		return []pb.Entry{}
	}
	return l.entries[l.stabled-l.first+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return
	}
	return l.entries[l.applied-l.first+1 : l.committed-l.first+1]
}

func (l *RaftLog) Entries(lo, hi uint64) ([]pb.Entry, error) {
	if lo < l.first {
		return nil, ErrCompacted
	}
	if hi > l.LastIndex()+1 {
		return nil, ErrUnavailable
	}
	return l.entries[lo-l.first : hi-l.first], nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.first-1 {
		return 0, ErrCompacted
	}
	if i == l.first-1 {
		return 0, nil
	}
	if i > l.LastIndex() {
		return 0, ErrCompacted
	}
	return l.entries[i-l.first].Term, nil
}

// appendEntries append new entries following prevLogIndex
func (l *RaftLog) appendEntries(prevLogIndex uint64, entries []*pb.Entry) {
	if len(entries) <= 0 {
		return
	}
	// compare
	if prevLogIndex < l.LastIndex() {
		i := 0
		for ; i < len(entries) && entries[i].Index <= l.LastIndex(); i++ {
			term, _ := l.Term(entries[i].Index)
			if entries[i].Term != term {
				// delete the conflict entries
				l.entries = l.entries[:entries[i].Index-l.first]
				l.stabled = entries[i].Index - 1
				break
			}
		}
		// all entries have been appended
		if i == len(entries) {
			entries = entries[:0]
		} else {
			entries = entries[i:]
		}
	}
	for _, entry := range entries {
		l.entries = append(l.entries, *entry)
	}
}
