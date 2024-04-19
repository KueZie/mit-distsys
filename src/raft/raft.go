package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"

	// "sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"

	"github.com/sasha-s/go-deadlock" // deadlock detection
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry is a single entry in the Raft log.
// This may be wrong
type LogEntry struct {
	Term    int
	Command interface{}
}

// Define FOLLOWER, CANDIDATE, and LEADER states.
const (
	FOLLOWER  = 0
	PRE_ELECTION_CANDIDATE = 1
	CANDIDATE = 2
	PENDING_LEADER_STARTUP = 3
	LEADER    = 4
)


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state on all servers
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile state on candidates
	votesReceived int // number of votes received

	// volatile state on leaders
	nextIndex  []int // index of the next log entry to send to each server
	matchIndex []int // index of highest log entry known to be replicated on server

	// configuration
	state           int           // follower, candidate, leader
	electionTimeout time.Duration // timeout for elections
	lastHeartbeat   time.Time     // time of last heartbeat

	// channels
	applyCh chan ApplyMsg
	hearbeatCh chan bool

	// logging
	logger *log.Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int // id of candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	CurrentTerm int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// we have received a heartbeat from the leader of some term
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeartbeat = time.Now()

	rf.logger.Printf("Received heartbeat from node %v, leader of term %v", args.LeaderId, args.Term)

	if args.Term < rf.currentTerm {
		rf.logger.Printf("Received heartbeat from node %v with lower term %v, but our term is %v", args.LeaderId, args.Term, rf.currentTerm)

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.logger.Printf("Received heartbeat from node %v with higher term %v, but our term is %v", args.LeaderId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term

		if rf.state != FOLLOWER {
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.votesReceived = 0
			rf.logger.Printf("Received AppendEntries from higher term leader while not follower, transiting to follower with state: term=%v, votedFor=%v, state=%v, votesReceived=%v", rf.currentTerm, rf.votedFor, rf.state, rf.votesReceived)
		}

		reply.Term = rf.currentTerm
		reply.Success = true
	}

	// we have received a heartbeat from the leader of the current term
	reply.Term = rf.currentTerm
	reply.Success = true
}

// Handle RequestVote RPCs
//  1. If the term of the candidate is less than our current term, we reject the vote.
//  2. If votedFor is null or the candidateId, and the candidate's log is at least as
//     up-to-date as ours, we grant the vote.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Printf("Received RequestVote from node %v", args.CandidateId)

	// if the candidate's term is less than our current term, we reject the vote
	if args.Term < rf.currentTerm {
		rf.logger.Printf("Rejecting RequestVote from node %v with lower term %v, but our term is %v", args.CandidateId, args.Term, rf.currentTerm)

		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.logger.Printf("Received RequestVote from node %v with higher term %v, but our term is %v", args.CandidateId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
		rf.votesReceived = 0
		rf.logger.Printf("Transition to follower with state: term=%v, votedFor=%v, state=%v, votesReceived=%v", rf.currentTerm, rf.votedFor, rf.state, rf.votesReceived)

		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// if we have already voted for someone else in this term, we reject the vote
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		rf.logger.Printf("Rejecting RequestVote from node %v, already voted for node %v", args.CandidateId, rf.votedFor)

		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	
	// if the candidate's log is at least as up-to-date as ours, we grant the vote
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term || 
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
		rf.logger.Printf("Granting vote to node %v", args.CandidateId)

		rf.votedFor = args.CandidateId
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	rf.logger.Fatalf("Something went wrong in RequestVote RPC handling.")
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	if rf.state != CANDIDATE {
		rf.logger.Fatalf("sendRequestVote called in state %v", rf.state)
	}
	rf.mu.Unlock()

	// We don't want to lock the mutex while sending RPCs, so we unlock it here
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		rf.logger.Printf("sendRequestVote to node %v failed, may have failed or network partition", server)
		return false
	}

	if reply.VoteGranted {
		rf.logger.Printf("Received vote from node %v", server)
		rf.votesReceived++
		return true
	}

	// if the vote was not granted, check if we need to update our term
	if rf.currentTerm < reply.CurrentTerm {
		rf.logger.Printf("Received higher term (%v) from node %v", reply.CurrentTerm, server)
		rf.currentTerm = reply.CurrentTerm
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	return false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.logger.Fatalf("sendAppendEntries called in state %v", rf.state)
	}
	rf.mu.Unlock()

	// We don't want to lock the mutex while sending RPCs, so we unlock it here
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Printf("Sending heartbeat to node %v", server)

	if !ok {
		rf.logger.Printf("sendAppendEntries to node %v failed, may have failed or network partition", server)
		return false
	}

	if reply.Success {
		rf.logger.Printf("Received success from node %v", server)
		return true
	}

	// if the vote was not granted, check if we need to update our term
	if rf.currentTerm < reply.Term {
		// we have received a higher term from another node while trying to send heartbeats
		// this means we are no longer the leader
		rf.logger.Printf("Received higher term (%v) from node %v", reply.Term, server)
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.logger.Printf("Transition to follower with state: term=%v, votedFor=%v, state=%v, votesReceived=%v", rf.currentTerm, rf.votedFor, rf.state, rf.votesReceived)
		return false
	}

	rf.logger.Printf("Node %v rejected heartbeat and we don't know why.", server)

	return false
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
	
		switch rf.state {
		case FOLLOWER:
			// check if we need to start an election
			if time.Since(rf.lastHeartbeat) > rf.electionTimeout {
				rf.logger.Printf("Starting election")
				rf.state = PRE_ELECTION_CANDIDATE
				rf.currentTerm++
				rf.votedFor = rf.me
			}
		case PRE_ELECTION_CANDIDATE:
			// PRE_ELECTION_CANDIDATE is a state that is used to prevent starting an election multiple times
			// in the same term. If we are in this state, we will transition to CANDIDATE in the next tick.
			rf.state = CANDIDATE
			rf.votesReceived = 1 // vote for self
			rf.lastHeartbeat = time.Now() // reset election timeout

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.sendRequestVote(i, &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}, &RequestVoteReply{})
			}
		case CANDIDATE:
			// if we have received a majority of votes, we become the leader
			if rf.votesReceived > len(rf.peers)/2 {
				// we have won the election for this term
				rf.state = PENDING_LEADER_STARTUP
				rf.votesReceived = 0
			}

			// if we have not received a majority of votes but the election timeout has passed,
			// we start a new election
			if time.Since(rf.lastHeartbeat) > rf.electionTimeout {
				rf.logger.Printf("Election timeout while candidate, restarting election")
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.state = PRE_ELECTION_CANDIDATE
			}
		case PENDING_LEADER_STARTUP:
			// we need to become leader and setup heartbeat routines
			rf.state = LEADER

			rf.logger.Printf("Transition to leader with state: term=%v, votedFor=%v, state=%v, votesReceived=%v", rf.currentTerm, rf.votedFor, rf.state, rf.votesReceived)
			
			// send heartbeats to all peers every 50ms
			go func() {
				rf.logger.Printf("Starting heartbeat routine")
				for {
					rf.mu.Lock()

					if (rf.killed() || rf.state != LEADER) {
						rf.logger.Printf("No longer leader in term %v, exiting heartbeat loop", rf.currentTerm)
						rf.mu.Unlock()
						return
					}

					rf.logger.Printf("Broadcast heartbeats in term %v", rf.currentTerm)
					for i := range rf.peers {
						if i == rf.me {
							continue
						}
						go rf.sendAppendEntries(i, &AppendEntriesArgs{
							Term:     rf.currentTerm,
							LeaderId: rf.me,
						}, &AppendEntriesReply{})
					}
					rf.mu.Unlock()

					time.Sleep(50 * time.Millisecond)
				}
			}()
		case LEADER:
			rf.logger.Printf("I am the leader of term %v", rf.currentTerm)
		}

		rf.mu.Unlock()

		tickDelta := time.Duration(50+rand.Intn(50)) * time.Millisecond
		time.Sleep(tickDelta)
	}	
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.state = FOLLOWER // start as a follower
	rf.log = append(rf.log, LogEntry{Term: 0}) // dummy entry

	// configuration
	rf.electionTimeout = time.Duration(600+rand.Intn(150)) * time.Millisecond // [600, 750) ms]
	rf.lastHeartbeat = time.Now()
	rf.votedFor = -1 // null

	// logging
	// stdout
	debugName := fmt.Sprintf("[raft-%v] ", me)
	if Debug {
		rf.logger = log.New(os.Stdout, debugName, log.Lmicroseconds)
	} else {
		// discard logs
		tmp, _ := os.Open(os.DevNull)
		defer tmp.Close()
		rf.logger = log.New(tmp, "", log.Lmicroseconds)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
