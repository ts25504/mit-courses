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

import "sync"
import "labrpc"

import "time"
import "math/rand"
// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	FOLLOWER = "Follower"
	CANDIDATE = "Candidate"
	LEADER = "Leader"
)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	logs          []LogEntry

	commitIndex   int
	lastApplied   int

	nextIndex     []int
	matchIndex    []int

	state         string
	heartbeatCh   chan bool
	leaderCh      chan bool
	commitCh      chan bool
	voteCount     int
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

func (rf *Raft) getLastIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.heartbeatCh <- true
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term

	if rf.state == CANDIDATE {
		rf.state = FOLLOWER
	}

	if rf.state == LEADER && args.Term > rf.currentTerm {
		rf.state = FOLLOWER
	}

	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	rf.logs = rf.logs[:args.PrevLogIndex+1]
	for i := 0; i < len(args.Entries); i++ {
		rf.logs = append(rf.logs, args.Entries[i])
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.getLastIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.getLastIndex()
		}
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.leaderCh <- false
			}
		}
	}
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	for n := rf.commitIndex + 1; n < rf.getLastIndex(); n++ {
		count := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] > n && rf.logs[n].Term == rf.currentTerm {
				count++
			}
		}

		if count > len(rf.peers) / 2 {
			rf.commitIndex = n
			rf.commitCh <- true
			break
		}
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			args.Entries = rf.logs[args.PrevLogIndex+1:]
			args.LeaderCommit = rf.commitIndex

			var reply AppendEntriesReply

			rf.sendAppendEntries(i, args, &reply)
			if rf.state != LEADER {
				break
			}
		}
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("RequestVote Error: args.Term < rf.currentTerm")
		return
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
			DPrintf("RequestVote Error: votedFor != -1 && votedFor != CandidateId")
			return
		}
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.state = FOLLOWER
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.currentTerm = reply.Term
		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers) / 2 {
				rf.state = LEADER
				rf.leaderCh <- true
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.state = FOLLOWER
				DPrintf("Term %d, Candidate %d: There is already a leader", rf.currentTerm, rf.me)
			}
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			var args RequestVoteArgs
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			var reply RequestVoteReply
			reply.Term = rf.currentTerm
			DPrintf("Term %d, Candidate %d: RequestVote to %d", rf.currentTerm, rf.me, i)
			rf.sendRequestVote(i, args, &reply)
			if rf.state != CANDIDATE {
				break
			}
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := rf.commitIndex
	term := rf.currentTerm
	isLeader := (rf.state == LEADER)

	if isLeader {
		var log LogEntry
		log.Term = rf.currentTerm
		log.Command = command
		log.Index = rf.getLastIndex() + 1
		DPrintf("Start log Term %d Index %d", log.Term, log.Index)
		rf.logs = append(rf.logs, log)
		index = log.Index
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) workAsFollower() {
	select {
	case <-rf.heartbeatCh:
		// DPrintf("Term %d, Follower %d: Heartbeat", rf.currentTerm, rf.me)
	case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
		rf.state = CANDIDATE
		DPrintf("Term %d, Follower %d: Election Timeout", rf.currentTerm, rf.me)
	}
}

func (rf *Raft) workAsCandidate() {
	rf.voteCount = 1
	rf.votedFor = rf.me
	rf.currentTerm++

	select {
	case <-rf.leaderCh:
		DPrintf("Term %d, Candidate %d: Empty the leaderCh", rf.currentTerm, rf.me)
	case <-rf.heartbeatCh:
		DPrintf("Term %d, Candidate %d: There is already a leader", rf.currentTerm, rf.me)
		rf.state = FOLLOWER
		return
	default:
	}

	go rf.broadcastRequestVote()

	select {
	case isLeader := <-rf.leaderCh:
		if isLeader {
			rf.state = LEADER
			DPrintf("Term %d, Candidate %d: Become the leader", rf.currentTerm, rf.me)
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.getLastIndex() + 1
				rf.matchIndex[i] = 0
			}
			go rf.broadcastAppendEntries()
		}
	case <-time.After(300 * time.Millisecond):
		DPrintf("Term %d, Candidate %d: Election timeout", rf.currentTerm, rf.me)
		if rf.state != LEADER {
			rf.state = FOLLOWER
		}
	}
}

func (rf *Raft) workAsLeader() {
	time.Sleep(10 * time.Millisecond)

	go rf.broadcastAppendEntries()
}

func (rf *Raft) work() {
	for {
		switch rf.state {
		case FOLLOWER:
			rf.workAsFollower()
		case CANDIDATE:
			rf.workAsCandidate()
		case LEADER:
			rf.workAsLeader()
		}
	}
}

func (rf *Raft) commit(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.commitCh:
			for i := rf.lastApplied+1; i <= rf.commitIndex; i++ {
				var msg ApplyMsg
				msg.Index = i
				msg.Command = rf.logs[i].Command
				DPrintf("Commit log Term %d Index %d", rf.logs[i].Term, msg.Index)
				applyCh <- msg
				rf.lastApplied = i
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{})
	rf.state = FOLLOWER
	rf.heartbeatCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	rf.commitCh = make(chan bool)
	rf.voteCount = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.work()
	go rf.commit(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
