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
//   should send an ApplyMsg to the service (or tester) in the same server.
//
import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"lab-raft/lablog"
	"lab-raft/labrpc"
)

//
// Type incecating the state of the server
type serverState int

//
// Constants for the 3 states of the server
const (
	follower serverState = iota
	leader
	candidate
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry. 
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type LogEntry struct {
	Entry interface{}
	Term  int
}

//
// A Go object implementing a single Raft peer.
type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state

	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

	applyCh chan ApplyMsg // The apply channel

	state serverState // The current state of the server

	
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries; each entry contains command for state machine,
	//and term when entry was received by leader

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	receivedHeartBeat bool
	votesReceived     int
	sendingCommands   bool
}

//
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	isleader = rf.state == leader
	term = rf.currentTerm

	return term, isleader
}


type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// Add more in lab 3
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}


type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// AppendRequest RPC request structure.
type AppendRequest struct {
	Term         int
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

//
// AppendReply RPC reply structure.
type AppendReply struct {
	Term              int  // currentTerm, for leader to update itself
	Success           bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	LogLen            int
	PrevIndexMismatch bool
}

//
// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lablog.RaftLog().
		Printf("%d got a vote request from %d", rf.me, args.CandidateId)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
	}

	logOk := true
	if len(rf.log) > 0 {
		logOk = (args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex+1 >= len(rf.log))
	}

	if args.Term == rf.currentTerm && logOk && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		lablog.RaftLog().
			Printf("%d granted vote to %d", rf.me, args.CandidateId)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		lablog.RaftLog().Printf("%d did NOT grant vote to %d", rf.me, args.CandidateId)
	}

}


func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs,
	reply *RequestVoteReply) bool {
	lablog.RaftLog().Printf("Candidate %d sending vote request to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	if ok {
		if reply.Term > rf.currentTerm {

			rf.currentTerm = reply.Term
			rf.state = follower
			rf.votedFor = -1
			rf.votesReceived = 0

			lablog.RaftLog().Printf("Candidate %d discovered its term is less than others", rf.me)
		} else if rf.state == candidate && reply.VoteGranted && reply.Term == rf.currentTerm {
			rf.votesReceived++

			lablog.RaftLog().Printf("%d received vote to %d and has now %d votes out of %d", rf.me, server, rf.votesReceived, len(rf.peers))

			if rf.votesReceived > len(rf.peers)/2 {
				lablog.RaftLog().Printf("%d became leader", rf.me)
				rf.state = leader

				// Reinitializing nextIndex and matchIndex
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0

					if i != rf.me {
						rf.mu.Unlock()
						rf.ReplicateLog(i)
						rf.mu.Lock()
					}
				}

				rf.mu.Unlock()
				rf.sendHeartBeat()
				rf.mu.Lock()
			}
		}
	}
	rf.mu.Unlock()
	return ok
}

// Receive Append
func (rf *Raft) AppendEntries(args *AppendRequest, reply *AppendReply) {
	rf.mu.Lock()
	if !rf.killed() {
		reply.PrevIndexMismatch = false
		if rf.currentTerm > args.Term {
			reply.Success = false
			lablog.RaftLog().Printf("Candidate %d: appending failed due to leader term being too small", rf.me)
		} else {
			rf.receivedHeartBeat = true
			rf.currentTerm = args.Term
			rf.state = follower

			if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Success = false
				reply.PrevIndexMismatch = true
				lablog.RaftLog().Printf("Candidate %d: appending failed due to differences in previous term", rf.me)
			} else if len(args.Entries) > 0 {
				if len(rf.log) > args.PrevLogIndex+1 {
					rf.log = rf.log[:args.PrevLogIndex+1]
				}
				rf.log = append(rf.log, args.Entries...)

				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = args.LeaderCommit
					lablog.RaftLog().Printf("%d commit index is now %d", rf.me, rf.commitIndex)
				}
				lablog.RaftLog().Printf("Follower %d: Entry after index %d appended. Log length is %d", rf.me, args.PrevLogIndex+1, len(rf.log))
				reply.Success = true
			} else {
				lablog.RaftLog().Printf("%d received heartbeat", rf.me)
				reply.Success = true

				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = args.LeaderCommit
					lablog.RaftLog().Printf("%d commit index is now %d", rf.me, rf.commitIndex)
				}
			}
		}

		reply.Term = rf.currentTerm
		reply.LogLen = len(rf.log)

	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppend(server int, args *AppendRequest,
	reply *AppendReply) bool {
	rf.mu.Lock()

	if rf.state == leader {

		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		rf.mu.Lock()
		if ok {
			if reply.Term > rf.currentTerm {
				rf.state = follower
				rf.votedFor = -1
				rf.votesReceived = 0
				rf.currentTerm = reply.Term
				lablog.RaftLog().Printf("Leader %d discovered its term is less than others", rf.me)
			} else {
				if reply.Success == true && !reply.PrevIndexMismatch {
					lablog.RaftLog().Printf("Updating next index of follower %d to %d", server, reply.LogLen)
					rf.nextIndex[server] = reply.LogLen
					rf.matchIndex[server] = reply.LogLen

					// We need to check what we can commit
					for i := rf.commitIndex + 1; i < len(rf.log); i++ {
						if rf.log[i].Term == rf.currentTerm {
							acks := 1
							for n := 0; n < len(rf.peers); n++ {
								if rf.matchIndex[n] > i && rf.me != n {
									acks++
								}
							}

							if acks > len(rf.peers)/2 {
								rf.commitIndex = i
								lablog.RaftLog().Printf("Leader %d: CommitIndex increased to %d", rf.me, rf.commitIndex)
							} else {
								break
							}
						}
					}

				} else if rf.state == leader {
					lablog.RaftLog().Printf("Leader needs to decrement nextIndex for %d", server)
					rf.nextIndex[server]--
					rf.mu.Unlock()
					go rf.ReplicateLog(server)
					rf.mu.Lock()
				}
			}
		}

		rf.mu.Unlock()
		return ok
	}

	rf.mu.Unlock()
	return false
}

//
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
	rf.mu.Lock()
	index := len(rf.log)
	term := rf.currentTerm
	rf.mu.Unlock()

	_, isLeader := rf.GetState()

	if isLeader {
		rf.mu.Lock()

		lablog.RaftLog().Printf("Leader %d received new command at index %d", rf.me, len(rf.log))
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})

		if !rf.sendingCommands {
			go rf.PeriodicallySendingCommands()
		}

		rf.mu.Unlock()

	}

	_, isLeader = rf.GetState()

	return index, term, isLeader
}

func (rf *Raft) PeriodicallySendingCommands() {
	time.Sleep(5 * time.Millisecond)
	go rf.sendHeartBeat()
	rf.mu.Lock()
	rf.sendingCommands = false
	rf.mu.Unlock()
}

func (rf *Raft) ReplicateLog(server int) {
	args := AppendRequest{}

	rf.mu.Lock()
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me

	if rf.nextIndex[server] > 0 {
		if rf.nextIndex[server] <= len(rf.log) {
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		} else {
			args.PrevLogIndex = len(rf.log) - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		}

	} else {
		args.PrevLogIndex = 0
		args.PrevLogTerm = 0
	}

	if args.PrevLogIndex != len(rf.log) {
		args.Entries = append(args.Entries, rf.log[args.PrevLogIndex+1:]...)
	}
	args.Term = rf.currentTerm
	rf.mu.Unlock()

	reply := AppendReply{}
	lablog.RaftLog().Printf("Leader %d will ask server %d to append from index %d", rf.me, server, args.PrevLogIndex+1)
	rf.sendAppend(server, &args, &reply)

}

func (rf *Raft) PeriodicallyCommitting() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex && rf.commitIndex < len(rf.log) {
			rf.lastApplied++
			lablog.RaftLog().Printf("Server %d committing index %d", rf.me, rf.lastApplied)
			rf.applyCh <- ApplyMsg{true, rf.log[rf.lastApplied].Entry, rf.lastApplied, rf.log[rf.lastApplied].Term}
		}
		rf.mu.Unlock()

		time.Sleep(20 * time.Millisecond)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if
// this server hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		rf.mu.Lock()
		rf.receivedHeartBeat = false
		rf.votedFor = -1
		rf.mu.Unlock()

		dur := 300 + rand.Intn(200)
		time.Sleep(time.Duration(dur) * time.Millisecond)

		_, isleader := rf.GetState()
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		} else if !rf.receivedHeartBeat && !isleader && rf.votedFor == -1 {
			rf.mu.Unlock()
			rf.startElections()
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartBeat() {

	for i := 0; i < len(rf.peers); i++ {
		_, isleader := rf.GetState()
		if i != rf.me && isleader {
			go rf.ReplicateLog(i)
		}
	}
}

func (rf *Raft) startElections() {
	rf.mu.Lock()
	lablog.RaftLog().Printf("%d starting elections for term %d", rf.me, rf.currentTerm+1)

	rf.currentTerm++
	rf.state = candidate
	rf.votedFor = rf.me
	rf.votesReceived = 1

	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	if len(rf.log) > 0 {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	} else {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	}

	args.Term = rf.currentTerm

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply)
		}
	}

	time.Sleep(3 * time.Second)

	rf.mu.Lock()
	if rf.state == candidate {
		rf.state = follower
		rf.votedFor = -1
		rf.votesReceived = 0
	}
	rf.mu.Unlock()

}

func (rf *Raft) Heartbeat() {
	for !rf.killed() {
		dur := 50
		time.Sleep(time.Duration(dur) * time.Millisecond)
		if rf.killed() {
			return
		} else {

			rf.mu.Lock()
			if rf.state == leader {
				rf.mu.Unlock()
				go rf.sendHeartBeat()
				rf.mu.Lock()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendVoteExample() {
	request := RequestVoteArgs{}
	reply := RequestVoteReply{}
	rf.mu.Lock()
	request.CandidateId = rf.me
	request.Term = -1
	lablog.RaftLog().Printf("%d sends %+v to %d", rf.me, request, rf.me)
	rf.mu.Unlock()

	if rf.sendRequestVote(rf.me, &request, &reply) {
		rf.mu.Lock()
		if reply.VoteGranted {
			lablog.RaftLog().Printf("%d got a vote", rf.me)
		}
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.state = follower
	rf.applyCh = applyCh

	rf.currentTerm = 1
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votesReceived = 0
	rf.log = append(rf.log, LogEntry{})
	rf.log[0].Term = 0
	rf.sendingCommands = false

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, len(rf.log))
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	lablog.RaftLog().Printf("%d was created", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.Heartbeat()
	go rf.PeriodicallyCommitting()

	return rf
}
