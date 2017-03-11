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
import "bytes"
import "encoding/gob"

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

	MAXSERVERCOUNT = 5
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
	snapshotCh    chan bool
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

func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getLastIncludeIndex() int {
	return rf.logs[0].Index
}

func (rf *Raft) getLastIncludeTerm() int {
	return rf.logs[0].Term
}

func (rf *Raft) convertToFollower(term int) {
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.votedFor = -1
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
	rf.mu.Unlock()
}

func (rf *Raft) snapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.getLastIncludeIndex())
	e.Encode(rf.getLastIncludeTerm())

	data := w.Bytes()

	if snapshot != nil {
		data = append(data, snapshot...)
	}

	rf.persister.SaveSnapshot(data)
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var lastIncludeIndex int
	var lastIncludeTerm int

	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)

	rf.installSnapshotToRaft(lastIncludeIndex, lastIncludeTerm)

	rf.snapshotCh <- true
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.heartbeatCh <- true

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.persister.SaveSnapshot(args.Data)
	rf.installSnapshotToRaft(args.LastIncludeIndex, args.LastIncludeTerm)

	rf.snapshotCh <- true

	reply.Term = rf.currentTerm
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && rf.state == LEADER {
		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			rf.persist()
		} else {
			rf.nextIndex[server] = args.LastIncludeIndex + 1
			rf.matchIndex[server] = args.LastIncludeIndex
		}
	}
	return ok
}

func (rf *Raft) installSnapshotToRaft(lastIncludeIndex int, lastIncludeTerm int) {
	var logs []LogEntry
	logs = append(logs, LogEntry{Term: lastIncludeTerm, Index: lastIncludeIndex})
	for i := len(rf.logs)-1; i >= 0; i-- {
		if rf.logs[i].Index == lastIncludeIndex && rf.logs[i].Term == lastIncludeTerm {
			logs = append(logs, rf.logs[i+1:]...)
			break
		}
	}

	rf.logs = logs

	rf.commitIndex = lastIncludeIndex
	rf.lastApplied = lastIncludeIndex
}

func (rf *Raft) discardOldLogEntries(index int) {
	var logs []LogEntry
	logs = append(logs, LogEntry{Index: index, Term: rf.logs[index-rf.getLastIncludeIndex()].Term})
	for i := index+1; i < len(rf.logs); i++ {
		logs = append(logs, rf.logs[i-rf.getLastIncludeIndex()])
	}

	rf.logs = logs
}

func (rf *Raft) StartSnapShot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.getLastIncludeIndex() || index > rf.getLastIndex() {
		return
	}

	rf.discardOldLogEntries(index)
	rf.persist()
	rf.snapshot(snapshot)
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
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) decreaseNextIndex(prevLogIndex int) int {
	nextIndex := 0
	for i := prevLogIndex - 1; i >= rf.getLastIncludeIndex(); i-- {
		if rf.logs[i-rf.getLastIncludeIndex()].Term != rf.logs[prevLogIndex-rf.getLastIncludeIndex()].Term {
			nextIndex = i + 1
			break
		}
	}
	return nextIndex
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.heartbeatCh <- true

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	if rf.logs[args.PrevLogIndex-rf.getLastIncludeIndex()].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = rf.decreaseNextIndex(args.PrevLogIndex)
		return
	}

	rf.logs = rf.logs[:args.PrevLogIndex+1-rf.getLastIncludeIndex()]
	for i := 0; i < len(args.Entries); i++ {
		rf.logs = append(rf.logs, args.Entries[i])
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.getLastIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.getLastIndex()
		}
		rf.commitCh <- true
	}

	reply.NextIndex = rf.getLastIndex() + 1
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && rf.state == LEADER {
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			if rf.currentTerm < reply.Term {
				rf.convertToFollower(reply.Term)
				rf.persist()
			} else {
				rf.nextIndex[server] = reply.NextIndex
			}
		}
	}
	return ok
}

func (rf *Raft) commitLogs() {
	commit := false
	for n := rf.commitIndex+1; n <= rf.getLastIndex(); n++ {
		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if rf.me != i && rf.matchIndex[i] >= n && rf.logs[n-rf.getLastIncludeIndex()].Term == rf.currentTerm {
				count++
			}
		}

		if count > len(rf.peers) / 2 {
			rf.commitIndex = n
			commit = true
		}
	}

	if commit {
		rf.commitCh <- true
	}
}

func (rf *Raft) broadcastHeartbeatJob() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.commitLogs()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == LEADER {
			if rf.nextIndex[i] > rf.getLastIncludeIndex() {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex-rf.getLastIncludeIndex()].Term
				args.Entries = rf.logs[args.PrevLogIndex+1-rf.getLastIncludeIndex():]
				args.LeaderCommit = rf.commitIndex

				var reply AppendEntriesReply

				go func(i int, args AppendEntriesArgs, reply AppendEntriesReply) {
					rf.sendAppendEntries(i, args, &reply)
				}(i, args, reply)
			} else {
				var args InstallSnapshotArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludeIndex = rf.getLastIncludeIndex()
				args.LastIncludeTerm = rf.getLastIncludeTerm()
				args.Data = rf.persister.ReadSnapshot()

				var reply InstallSnapshotReply

				go func(i int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
					rf.sendInstallSnapshot(i, args, &reply)
				}(i, args, reply)
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

func (rf *Raft) checkLogUpToDate(candidateLastTerm int, candidateLastIndex int) bool {
	upToDate := false
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()

	if candidateLastTerm > lastTerm {
		upToDate = true
	}

	if candidateLastTerm == lastTerm && candidateLastIndex >= lastIndex {
		upToDate = true
	}

	return upToDate
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Term %d, Follower %d: RequestVote Error: args.Term < rf.currentTerm", rf.currentTerm, rf.me)
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	upToDate := rf.checkLogUpToDate(args.LastLogTerm, args.LastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Term %d, Follower %d: RequestVote Error: votedFor != -1 && votedFor != CandidateId || !upToDate, votedFor=%d", rf.currentTerm, rf.me, rf.votedFor)
	}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && rf.state == CANDIDATE {
		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers) / 2 {
				rf.state = LEADER
				rf.leaderCh <- true
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				rf.persist()
				DPrintf("Term %d, Candidate %d: There is already a leader", rf.currentTerm, rf.me)
			}
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()
	var reply RequestVoteReply

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == CANDIDATE {
			DPrintf("Term %d, Candidate %d: RequestVote to %d", rf.currentTerm, rf.me, i)
			go func(i int, args RequestVoteArgs, reply RequestVoteReply) {
				rf.sendRequestVote(i, args, &reply)
			}(i, args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.commitIndex
	term := rf.currentTerm
	isLeader := (rf.state == LEADER)

	if isLeader {
		var log LogEntry
		log.Term = rf.currentTerm
		log.Command = command
		log.Index = rf.getLastIndex() + 1
		DPrintf("Server %d: Start log Term %d Index %d Command %v", rf.me, log.Term, log.Index, log.Command)
		rf.logs = append(rf.logs, log)
		rf.persist()
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
		rf.mu.Lock()
		rf.state = CANDIDATE
		DPrintf("Term %d, Follower %d: Election Timeout", rf.currentTerm, rf.me)
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.voteCount = 1
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.persist()
	rf.mu.Unlock()

	rf.broadcastRequestVote()

}

func (rf *Raft) workAsCandidate() {
	rf.startElection()

	select {
	case isLeader := <-rf.leaderCh:
		if isLeader {
			rf.mu.Lock()
			rf.state = LEADER
			DPrintf("Term %d, Candidate %d: Become the leader", rf.currentTerm, rf.me)
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.getLastIndex() + 1
				rf.matchIndex[i] = 0
			}
			rf.mu.Unlock()
			rf.broadcastHeartbeatJob()
		}
	case <-rf.heartbeatCh:
		rf.mu.Lock()
		DPrintf("Term %d, Candidate %d: Become follower", rf.currentTerm, rf.me)
		rf.state = FOLLOWER
		rf.mu.Unlock()
	case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
		rf.mu.Lock()
		DPrintf("Term %d, Candidate %d: Election timeout", rf.currentTerm, rf.me)
		rf.mu.Unlock()
	}
}

func (rf *Raft) workAsLeader() {
	time.Sleep(50 * time.Millisecond)
	rf.broadcastHeartbeatJob()
}

func (rf *Raft) work() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case FOLLOWER:
			rf.workAsFollower()
		case CANDIDATE:
			rf.workAsCandidate()
		case LEADER:
			rf.workAsLeader()
		}
	}
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.commitCh:
			rf.mu.Lock()
			commitIndex := rf.commitIndex
			for i := rf.lastApplied+1; i <= commitIndex; i++ {
				var msg ApplyMsg
				msg.Index = i
				msg.Command = rf.logs[i-rf.getLastIncludeIndex()].Command
				DPrintf("Server %d: Commit log Term %d Index %d Command %v", rf.me,  rf.logs[i-rf.getLastIncludeIndex()].Term, msg.Index, msg.Command)
				applyCh <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		case <-rf.snapshotCh:
			rf.mu.Lock()
			var msg ApplyMsg
			msg.UseSnapshot = true
			msg.Snapshot = rf.persister.ReadSnapshot()
			DPrintf("Server %d: Install Snapshot")
			applyCh <- msg
			rf.mu.Unlock()
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
	rf.heartbeatCh = make(chan bool, MAXSERVERCOUNT)
	rf.leaderCh = make(chan bool, MAXSERVERCOUNT)
	rf.commitCh = make(chan bool, MAXSERVERCOUNT)
	rf.snapshotCh = make(chan bool, MAXSERVERCOUNT)
	rf.voteCount = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.work()
	go rf.apply(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	return rf
}
