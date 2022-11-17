//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me", its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = false

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

const (
	Leader = iota
	Candidate
	Follower
)

//
// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
//
type ApplyCommand struct {
	Index   int
	Command interface{}
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// AppendEntrieArgs
// ===============
//
// Example AppendEntries RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int        //leader's term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry log entries to store (empty for heartbeat; may send more than one for efficiency)
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader's commitIndex
}

//
// AppendEntriesReply
// ================
//
// Example AppendEntries RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term      int  //currentTerm, for leader to update itself
	Success   bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	NextIndex int
}

type AppendEntriesArgsReply struct {
	peer  int
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	argsReply := &AppendEntriesArgsReply{
		args:  args,
		reply: reply,
	}
	rf.appendEntriesInit <- argsReply
	<-rf.appendEntriesFinish
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	if ok {
		argsReply := &AppendEntriesArgsReply{
			peer:  peer,
			args:  args,
			reply: reply,
		}
		rf.sendAppendEntriesChan <- argsReply
	}
	return ok
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Piazza or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain

	status      int //whether it is Leader, Candidate, or Follower
	votedAmount int

	//persistent state on all servers
	currentTerm int        //latest term server has seen (initialized to 0, increases monotonically)
	votedFor    int        //candidateId that received vote in term (or -1 if none)
	log         []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//volatile state on all servers
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//volatile state on leaders
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	//------------------------------------------------------
	closeChan       chan bool
	getStateRequest chan bool
	getStateReturn  chan *State

	appendEntriesInit     chan *AppendEntriesArgsReply
	appendEntriesFinish   chan bool
	sendAppendEntriesChan chan *AppendEntriesArgsReply

	requestVoteInit     chan *RequestVoteArgsReply
	requestVoteFinish   chan bool
	sendRequestVoteChan chan *RequestVoteArgsReply

	putCommandInit   chan interface{}
	putCommandFinish chan *PutCommandReply

	applyCh chan ApplyCommand
}

type State struct {
	me       int
	term     int
	isleader bool
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {
	var me int
	var term int
	var isleader bool
	// Your code here (2A)
	rf.getStateRequest <- true
	s := <-rf.getStateReturn
	me = s.me
	term = s.term
	isleader = s.isleader
	return me, term, isleader
}

type PutCommandReply struct {
	term     int
	index    int
	isLeader bool
}

//
// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	Term         int //candidate's term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

//
// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
//
//
type RequestVoteReply struct {
	// Your data here (2A)
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

type RequestVoteArgsReply struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	argsReply := &RequestVoteArgsReply{
		args:  args,
		reply: reply,
	}
	rf.requestVoteInit <- argsReply
	<-rf.requestVoteFinish
}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		argsReply := &RequestVoteArgsReply{
			args:  args,
			reply: reply,
		}
		rf.sendRequestVoteChan <- argsReply
	}
	return ok
}

//
// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
//
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	isLeader := false
	_, _, isLeader = rf.GetState()
	if isLeader {
		rf.putCommandInit <- command
		r := <-rf.putCommandFinish
		if r.isLeader {
			return r.index, r.term, true
		} else {
			return -1, -1, false
		}
	} else {
		return -1, -1, false
	}

}

//
// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Stop() {
	// Your code here, if desired
	rf.closeChan <- true
}

//
// NewPeer
// ====
//
// The service or tester wants to create a Raft server
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
//
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// Your initialization code here (2A, 2B)

	rf.status = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	var log []LogEntry
	rf.log = log
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.getStateRequest = make(chan bool)
	rf.getStateReturn = make(chan *State)
	rf.closeChan = make(chan bool)
	rf.appendEntriesInit = make(chan *AppendEntriesArgsReply)
	rf.appendEntriesFinish = make(chan bool)
	rf.sendAppendEntriesChan = make(chan *AppendEntriesArgsReply)
	rf.requestVoteInit = make(chan *RequestVoteArgsReply)
	rf.requestVoteFinish = make(chan bool)
	rf.sendRequestVoteChan = make(chan *RequestVoteArgsReply)
	rf.putCommandInit = make(chan interface{})
	rf.putCommandFinish = make(chan *PutCommandReply)
	rf.applyCh = applyCh

	go rf.MainRoutine()

	return rf
}

//main routine that takes care of all server business
func (rf *Raft) MainRoutine() {
	for {
		select {
		case <-rf.closeChan:
			return
		case <-rf.getStateRequest:
			isLeader := rf.status == Leader
			states := &State{
				me:       rf.me,
				term:     rf.currentTerm,
				isleader: isLeader,
			}
			rf.getStateReturn <- states
		default:
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				msg := ApplyCommand{
					Index:   rf.lastApplied,
					Command: rf.log[rf.lastApplied-1].Command,
				}
				rf.applyCh <- msg
			}
			switch rf.status {
			case Leader:
				rf.LeaderStuff()
			case Follower:
				rf.FollowerStuff()
			case Candidate:
				rf.CandidateStuff()
			}
		}
	}
}

//what happens when it's the leader
func (rf *Raft) LeaderStuff() {
	if len(rf.log) > 0 {
		for l := rf.log[len(rf.log)-1].Index; l > rf.commitIndex; l-- {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= l && rf.log[l-1].Term == rf.currentTerm {
					count++
				}
			}
			if count*2 > len(rf.peers) {
				rf.commitIndex = l
				break
			}
		}
	}
	timer := time.NewTimer(time.Duration(100) * time.Millisecond)

	select {
	case <-rf.requestVoteInit:
		rf.requestVoteFinish <- true
	case <-rf.sendRequestVoteChan:
	case argsReply := <-rf.appendEntriesInit:
		rf.dealWithAppendEntries(argsReply)
	case argsReply := <-rf.sendAppendEntriesChan:
		reply := argsReply.reply
		if reply.Term > rf.currentTerm {
			rf.status = Follower
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.votedAmount = 0
			return
		}

		args := argsReply.args

		if !reply.Success {
			rf.nextIndex[argsReply.peer] = reply.NextIndex
		}
		if reply.Success && (len(args.Entries) > 0) {
			rf.nextIndex[argsReply.peer] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[argsReply.peer] = args.Entries[len(args.Entries)-1].Index
		}

	case command := <-rf.putCommandInit:
		index := 1
		if len(rf.log) > 0 {
			index = rf.log[len(rf.log)-1].Index + 1
		}

		rf.putCommandFinish <- &PutCommandReply{
			index:    index,
			term:     rf.currentTerm,
			isLeader: true,
		}

		newEntry := LogEntry{
			Index:   index,
			Term:    rf.currentTerm,
			Command: command,
		}

		rf.log = append(rf.log, newEntry)

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			prevTerm := 0
			prevIndex := 0
			if rf.nextIndex[i] > 1 {
				prevIndex = rf.nextIndex[i] - 1
				prevTerm = rf.log[prevIndex-1].Term
			}

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
			}

			if rf.log[len(rf.log)-1].Index >= rf.nextIndex[i] {
				var entries []LogEntry
				for i := args.PrevLogIndex; i < rf.log[len(rf.log)-1].Index; i++ {
					entries = append(entries, rf.log[i])
				}
				args.Entries = entries
			}
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}

	case <-timer.C:

		lastIndex := 0
		if len(rf.log) > 0 {
			lastIndex = rf.log[len(rf.log)-1].Index
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			prevTerm := 0
			prevIndex := 0
			if rf.nextIndex[i] > 1 {
				prevIndex = rf.nextIndex[i] - 1
				prevTerm = rf.log[prevIndex-1].Term
			}

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
			}

			if lastIndex >= rf.nextIndex[i] {
				var entries []LogEntry
				for i := args.PrevLogIndex; i < lastIndex; i++ {
					entries = append(entries, rf.log[i])
				}
				args.Entries = entries
			}
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
		return

	}
}

//what happens when it's a candidate
func (rf *Raft) CandidateStuff() {
	timer := time.NewTimer(time.Duration(rand.Intn(300)+300) * time.Millisecond)
	select {
	case argsReply := <-rf.requestVoteInit:
		rf.dealWithRequestVote(argsReply)
	case argsReply := <-rf.sendRequestVoteChan:
		reply := argsReply.reply

		if reply.Term > rf.currentTerm {
			rf.status = Follower
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.votedAmount = 0
			return
		}

		if reply.VoteGranted && (reply.Term == rf.currentTerm) {
			rf.votedAmount++
			if rf.votedAmount*2 > len(rf.peers) {
				rf.status = Leader
				index := 0
				if len(rf.log) > 0 {
					index = rf.log[len(rf.log)-1].Index
				}

				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))

				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = index + 1
					rf.matchIndex[i] = 0
				}
			}
		}
	case argsReply := <-rf.appendEntriesInit:
		rf.dealWithAppendEntries(argsReply)
	case <-rf.sendAppendEntriesChan:
	case <-rf.putCommandInit:
		rf.putCommandFinish <- &PutCommandReply{}
	case <-timer.C:
		rf.status = Candidate
		rf.votedAmount = 1
		rf.votedFor = rf.me
		rf.currentTerm += 1

		index := 0
		term := 0
		if len(rf.log) > 0 {
			index = rf.log[len(rf.log)-1].Index
			term = rf.log[len(rf.log)-1].Term
		}

		for p, _ := range rf.peers {
			if (rf.me == p) || (rf.status != Candidate) {
				continue
			}
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: index,
				LastLogTerm:  term,
			}
			go rf.sendRequestVote(p, args, &RequestVoteReply{})
		}
	}
}

//what happens when it's a follower
func (rf *Raft) FollowerStuff() {
	timer := time.NewTimer(time.Duration(rand.Intn(300)+300) * time.Millisecond)
	select {
	case argsReply := <-rf.requestVoteInit:
		rf.dealWithRequestVote(argsReply)
	case <-rf.sendRequestVoteChan:
	case argsReply := <-rf.appendEntriesInit:
		rf.dealWithAppendEntries(argsReply)
	case <-rf.sendAppendEntriesChan:
	case <-rf.putCommandInit:
		rf.putCommandFinish <- &PutCommandReply{}
	case <-timer.C:
		rf.status = Candidate
		rf.votedAmount = 1
		rf.votedFor = rf.me
		rf.currentTerm += 1

		index := 0
		term := 0
		if len(rf.log) > 0 {
			index = rf.log[len(rf.log)-1].Index
			term = rf.log[len(rf.log)-1].Term
		}

		for p, _ := range rf.peers {
			if (rf.me == p) || (rf.status != Candidate) {
				continue
			}
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: index,
				LastLogTerm:  term,
			}
			go rf.sendRequestVote(p, args, &RequestVoteReply{})
		}
	}
}

//what happens when AppendEntries is called
//put as a separate method to avoid race conditions
func (rf *Raft) dealWithAppendEntries(argsReply *AppendEntriesArgsReply) {
	index := 0
	if len(rf.log) > 0 {
		index = rf.log[len(rf.log)-1].Index
	}
	args := argsReply.args
	reply := argsReply.reply

	if args.Term < rf.currentTerm {
		reply.NextIndex = index + 1
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.appendEntriesFinish <- true
		return
	}
	rf.status = Follower
	rf.votedFor = -1
	rf.currentTerm = args.Term
	rf.votedAmount = 0

	reply.Term = rf.currentTerm

	if args.PrevLogIndex > len(rf.log) {
		reply.Success = false
		reply.NextIndex = index + 1
		rf.appendEntriesFinish <- true
		return
	}

	if args.PrevLogIndex > 0 {
		if args.PrevLogTerm != rf.log[args.PrevLogIndex-1].Term {
			index := args.PrevLogIndex
			for i := 0; i < args.PrevLogIndex; i++ {
				if rf.log[i].Term == rf.log[args.PrevLogIndex-1].Term {
					index = i + 1
					break
				}
			}
			reply.NextIndex = index
			rf.appendEntriesFinish <- true
			return
		}
	}

	reply.Success = true
	lastIndex := 0
	if len(rf.log) > 0 {
		lastIndex = rf.log[len(rf.log)-1].Index
	}
	reply.NextIndex = lastIndex + 1

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}

	if len(args.Entries) >= 0 {
		rf.log = append(rf.log[0:args.PrevLogIndex], args.Entries...)
	}

	rf.appendEntriesFinish <- true
}

//what happens when RequestVote is called
//put as a separate method to avoid race conditions
func (rf *Raft) dealWithRequestVote(argsReply *RequestVoteArgsReply) {
	args := argsReply.args
	reply := argsReply.reply

	b := false

	if len(rf.log) > 0 {
		l := rf.log[len(rf.log)-1]
		if (args.LastLogTerm > l.Term) || (args.LastLogTerm == l.Term && args.LastLogIndex >= l.Index) {
			b = true
		}
	} else {
		b = true
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
		if b {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && b {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.status = Follower
	}

	reply.Term = rf.currentTerm
	rf.requestVoteFinish <- true
	return
}
