package raft

import (
	"6.824/labgob"
	"bytes"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"6.824/labrpc"
)

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

const (
	HEART_BEAT_TIMEOUT = 100
)

type LogEntry struct {
	Term    int         // Term number when created
	Command interface{} // Command to be excuted
}

// InstallSnapshotArgs is the InstallSnapshot RPC arguments structure.
// field names must start with capital letters!
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

// InstallSnapshotReply is the InstallSnapshot RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state         RaftState
	lastHeartbeat time.Time // the timestamp of the last heartbeat message

	// Persistent state on all servers
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader

	// Volatile state on all servers
	commitIndex int           // index of highest log entry known to be committed
	lastApplied int           // index of highest log entry applied to state machine
	applyCh     chan ApplyMsg // the channel on which the tester or service expects Raft to send ApplyMsg messages

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// Snapshot state on all servers
	lastIncludedIndex int    // 快照（snapshot）替换了所有该索引及之前的日志条目，整个日志直到这个索引都会被丢弃。在恢复时，系统将从该索引处开始重新应用日志。
	lastIncludedTerm  int    // term of lastIncludedIndex
	snapshot          []byte // 被压缩后的快照信息

	// 临时位置，用于将快照snapshot传递给应用线程
	// All apply messages should be sent in one go routine, we need the temporary space for applyLogsLoop to handle the snapshot apply
	waitingIndex    int    // lastIncludedIndex to be sent to applyCh
	waitingTerm     int    // lastIncludedTerm to be sent to applyCh
	waitingSnapshot []byte // snapshot to be sent to applyCh
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // with leaderId follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // The length of the currently synchronized node's log
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// code to send an InstallSnapshot RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	index = len(rf.log) + rf.lastIncludedIndex
	term = rf.currentTerm
	Debug(dLog, "S%d Add command at T%d. LI: %d, Command: %v\n", rf.me, term, index, command)
	rf.persist()
	rf.sendEntries(false)

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
	Debug(dClient, "S%d Current client is exiting.", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		electionTimeout := time.Duration(HEART_BEAT_TIMEOUT*2+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
		rf.mu.Lock()
		// If election timeout elapses: start new election
		if rf.state != Leader && time.Since(rf.lastHeartbeat) >= electionTimeout {
			Debug(dTimer, "S%d ELT elapsed. Converting to Candidate, calling election.", rf.me)
			rf.startElection()
		}
		// leader repeat heartbeat during idle periods to prevent election timeouts (§5.2)
		if rf.state == Leader {
			Debug(dTimer, "S%d HBT elapsed. Broadcast heartbeats.", rf.me)
			rf.sendEntries(true)
		}
		rf.mu.Unlock()
		//如果没有这个休眠操作，这个 goroutine 就会立即进入下一次循环，导致任务过于频繁
		//你需要编写定时执行或延迟一段时间后执行某些操作的代码。最简单的方法是创建一个带有循环的 goroutine，并在循环中调用 time.Sleep()
		time.Sleep(time.Duration(HEART_BEAT_TIMEOUT) * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.lastHeartbeat = time.Now()
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for peer := range rf.peers {
		rf.nextIndex[peer] = 1
	}
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	lastLogIndex, lastLogTerm := rf.lastLogInfo()
	Debug(dClient, "S%d Started at T%d. LLI: %d, LLT: %d.", rf.me, rf.currentTerm, lastLogIndex, lastLogTerm)
	// start ticker goroutine to start elections
	go rf.ticker()

	// Apply logs periodically until the last committed index to make sure state machine is up to date.
	go rf.applyLogsLoop()

	return rf
}

// 不断检查rf.commitIndex > rf.lastApplied, 将rf.lastApplied递增然后发送到管道applyCh
func (rf *Raft) applyLogsLoop() {
	for !rf.killed() {
		// Apply logs periodically until the last committed index.
		rf.mu.Lock()
		// To avoid the apply operation getting blocked with the lock held,
		// use a slice to store all committed messages to apply, and apply them only after unlocked
		var appliedMsgs []ApplyMsg
		rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
		//检查是否存在等待应用的快照:如果不为nil，说明有快照snapshot要被提交
		//如果存在等待应用的快照，则将其应用；
		//否则，依次应用从 rf.lastApplied 到 rf.commitIndex 之间的所有日志
		if rf.waitingSnapshot != nil {
			appliedMsgs = append(appliedMsgs, ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.waitingSnapshot,
				SnapshotTerm:  rf.waitingTerm,
				SnapshotIndex: rf.waitingIndex,
			})
			rf.waitingSnapshot = nil
		} else {
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.getEntry(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
				})
				Debug(dLog2, "S%d Applying log at T%d. LA: %d, CI: %d.", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			}
		}
		rf.mu.Unlock()
		for _, msg := range appliedMsgs {
			rf.applyCh <- msg
		}
		time.Sleep(time.Duration(HEART_BEAT_TIMEOUT) * time.Millisecond)
	}
}

/*====================选举流程=====================================*/
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++    // Increment currentTerm
	rf.votedFor = rf.me // Vote for self
	rf.persist()
	rf.lastHeartbeat = time.Now()
	Debug(dTimer, "S%d Resetting ELT because of election, wait for next potential election timeout.", rf.me)
	lastLogIndex, lastLogTerm := rf.lastLogInfo()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	voteCount := 1
	var once sync.Once
	// Send RequestVote RPCs to all other servers
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		Debug(dVote, "S%d -> S%d Sending request vote at T%d.", rf.me, peer, rf.currentTerm)
		go rf.candidateRequestVote(&voteCount, args, &once, peer) //Send RequestVote RPCs to all other servers
	}

}

// 主语是Candidate
func (rf *Raft) candidateRequestVote(voteCount *int, args *RequestVoteArgs, once *sync.Once, server int) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dVote, "S%d <- S%d Received request vote reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dVote, "S%d Term is lower, invalid vote reply. (%d < %d)", rf.me, reply.Term, rf.currentTerm)
			return
		}
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the vote request, vote reply discarded. "+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}
		// If AppendEntries RPC received from new leader: convert to follower
		rf.checkTerm(reply.Term)
		if reply.VoteGranted {
			*voteCount++
			Debug(dVote, "S%d <- S%d Get a yes vote at T%d.", rf.me, server, rf.currentTerm)
			// If votes received from majority of servers: become leader
			if *voteCount > len(rf.peers)/2 {
				once.Do(func() {
					Debug(dLeader, "S%d Received majority votes at T%d. Become leader.", rf.me, rf.currentTerm)
					rf.state = Leader
					//成为Leader之后初始化nextIndex和matchIndex
					lastLogIndex, _ := rf.lastLogInfo()
					for peer := range rf.peers {
						rf.nextIndex[peer] = lastLogIndex + 1 //(initialized to leader last log index + 1
						rf.matchIndex[peer] = 0               //initialized to 0, increases monotonically
					}
					// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
					rf.sendEntries(true)
				})
			}
		} else {
			Debug(dVote, "S%d <- S%d Get a no vote at T%d.", rf.me, server, rf.currentTerm)
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote, "S%d <- S%d Received vote request at T%d.", rf.me, args.CandidateId, rf.currentTerm)
	reply.VoteGranted = false
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d Term is lower, rejecting the vote. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	lastLogIndex, lastLogTerm := rf.lastLogInfo()
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)) {
		Debug(dVote, "S%d Granting vote to S%d at T%d.", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		Debug(dTimer, "S%d Resetting ELT, wait for next potential election timeout.", rf.me)
		rf.lastHeartbeat = time.Now()
	}
}

// 检查当前服务器的任期是不是比另外那台服务器的任期小，一旦发现当前服务器的任期数字较小，我们就选择转变为 Follower 状态，并更新任期
// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) checkTerm(term int) bool {
	if rf.currentTerm < term {
		rf.state = Follower
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
		return true
	}
	return false
}

/*====================心跳流程=========================================*/
func (rf *Raft) sendEntries(isHeartbeat bool) {
	lastLogIndex, _ := rf.lastLogInfo()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[peer]
		if nextIndex <= rf.lastIncludedIndex {
			// current leader does not have enough log to sync the outdated peer,
			// because logs were cleared after the snapshot, then send an InstallSnapshot RPC instead
			rf.sendSnapshot(peer)
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.getEntry(nextIndex - 1).Term,
			LeaderCommit: rf.commitIndex,
		}
		if lastLogIndex >= nextIndex {
			// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			//如果leader的Last log index ≥ nextIndex[peer]
			args.Entries = rf.getSlice(nextIndex, lastLogIndex+1)
			Debug(dLog, "S%d -> S%d Sending append entries at T%d. PLI: %d, PLT: %d, LC: %d. Entries: %v.",
				rf.me, peer, rf.currentTerm, args.PrevLogIndex,
				args.PrevLogTerm, args.LeaderCommit, args.Entries,
			)
			go rf.leaderSendEntries(args, peer)
		} else if isHeartbeat { //心跳就是一种特殊的AppendEntries, 其特殊在Entries长度为0
			args.Entries = make([]LogEntry, 0)
			Debug(dLog, "S%d -> S%d Sending heartbeat at T%d. PLI: %d, PLT: %d, LC: %d.",
				rf.me, peer, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			go rf.leaderSendEntries(args, peer)
		}
	}
}

func (rf *Raft) leaderSendEntries(args *AppendEntriesArgs, server int) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dLog, "S%d <- S%d Received send entry reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term lower, invalid send entry reply. (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			return
		}
		//必须得保证任期一致！
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the append request, send entry reply discarded. "+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}
		//如果leader的任期比对方的小，变成follower，跟上对方的任期，然后直接退出，没有下一步动作了
		if rf.checkTerm(reply.Term) {
			return
		}
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		if reply.Success {
			Debug(dLog, "S%d <- S%d Log entries in sync at T%d.", rf.me, server, rf.currentTerm)
			newNext := args.PrevLogIndex + 1 + len(args.Entries)
			newMatch := args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = max(newNext, rf.nextIndex[server])
			rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])
			// 更新Leader的commitIndex:If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
			// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
			for N := rf.lastIncludedIndex + len(rf.log); N > rf.commitIndex && rf.getEntry(N).Term == rf.currentTerm; N-- {
				count := 1
				for peer, matchIndex := range rf.matchIndex {
					if peer == rf.me {
						continue
					}
					if matchIndex >= N {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					rf.commitIndex = N
					Debug(dCommit, "S%d Updated commitIndex at T%d for majority consensus. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
					break
				}
			}
		} else {
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
			// the optimization that backs up nextIndex by more than one entry at a time
			if reply.XTerm == -1 {
				// follower's log is too short， nextIndex = XLen
				rf.nextIndex[server] = reply.XLen + 1
			} else {
				_, maxIndex := rf.getBoundsWithTerm(reply.XTerm)
				if maxIndex != -1 {
					// leader has XTerm， nextIndex = leader's last entry for XTerm
					rf.nextIndex[server] = maxIndex
				} else {
					// leader doesn't have XTerm，nextIndex = XIndex
					rf.nextIndex[server] = reply.XIndex
				}
			}
			//If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			lastLogIndex, _ := rf.lastLogInfo()
			nextIndex := rf.nextIndex[server]
			if nextIndex <= rf.lastIncludedIndex {
				// current leader does not have enough log to sync the outdated peer,
				// because logs were cleared after the snapshot, then send an InstallSnapshot RPC instead
				rf.sendSnapshot(server)
			} else if lastLogIndex >= nextIndex {
				Debug(dLog, "S%d <- S%d Inconsistent logs, retrying.", rf.me, server)
				newArg := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  rf.getEntry(nextIndex - 1).Term,
					Entries:      rf.getSlice(nextIndex, lastLogIndex+1),
				}
				go rf.leaderSendEntries(newArg, server)
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		Debug(dLog2, "S%d <- S%d Received heartbeat at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	} else {
		Debug(dLog2, "S%d <- S%d Received append entries at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	}
	reply.Success = false
	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		Debug(dLog2, "S%d Term is lower, rejecting append request. (%d < %d)",
			rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}
	// For Candidates. If AppendEntries RPC received from new leader: convert to follower
	if rf.state == Candidate && rf.currentTerm == args.Term {
		rf.state = Follower
		Debug(dLog2, "S%d Convert from candidate to follower at T%d.", rf.me, rf.currentTerm)
	}
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	rf.lastHeartbeat = time.Now()

	if args.PrevLogIndex < rf.lastIncludedIndex {
		alreadySnapshotLogLen := rf.lastIncludedIndex - args.PrevLogIndex
		if alreadySnapshotLogLen <= len(args.Entries) {
			newArgs := &AppendEntriesArgs{
				Term:         args.Term,
				LeaderId:     args.LeaderId,
				PrevLogTerm:  rf.lastIncludedTerm,
				PrevLogIndex: rf.lastIncludedIndex,
				Entries:      args.Entries[alreadySnapshotLogLen:],
				LeaderCommit: args.LeaderCommit,
			}
			args = newArgs
			Debug(dWarn, "S%d Log entry at PLI already discarded by snapshot, readjusting. PLI: %d, PLT:%d, Entries: %v.",
				rf.me, args.PrevLogIndex, args.Entries)
		} else {
			Debug(dWarn, "S%d Log entry at PLI already discarded by snapshot, assume as a match. PLI: %d.", rf.me, args.PrevLogIndex)
			reply.Success = true
			return
		}
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	//在被同步节点尝试对比 Leader 节点发来的 PrevLogTerm 和 PrevLogIndex 时，如果发生不一致，我们顺带把冲突信息带上
	if args.PrevLogTerm == -1 || args.PrevLogTerm != rf.getEntry(args.PrevLogIndex).Term {
		Debug(dDrop, "S%d Prev log entries do not match. Ask leader to retry.", rf.me)
		reply.XLen = len(rf.log) + rf.lastIncludedIndex
		reply.XTerm = rf.getEntry(args.PrevLogIndex).Term
		reply.XIndex, _ = rf.getBoundsWithTerm(reply.XTerm)
		return
	}
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	for i, entry := range args.Entries {
		if rf.getEntry(i+1+args.PrevLogIndex).Term != entry.Term {
			// Append any new entries not already in the log:1+args.PrevLogIndex是冲突日志的开始位置（也等于leader中的nextIndex[peer]）
			rf.log = append(rf.getSlice(1+rf.lastIncludedIndex, i+1+args.PrevLogIndex), args.Entries[i:]...)
			break
		}
	}
	Debug(dLog2, "S%d <- S%d Append entries success. Saved logs: %v.", rf.me, args.LeaderId, args.Entries)
	if len(args.Entries) > 0 {
		rf.persist()
	}

	//更新Follower的commitIndex：If leaderCommit > commitIndex, set commitIndex=min(leaderCommit, index of last new entry)
	//index of last new entry：不是当前所有日志条目的最后一个日志下标，而是这次 AppendEntries 提交的日志应用后，在这批日志中对应的最后一个日志下标
	if args.LeaderCommit > rf.commitIndex {
		Debug(dCommit, "S%d Get higher LC at T%d, updating commitIndex. (%d < %d)",
			rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		Debug(dCommit, "S%d Updated commitIndex at T%d. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
	}
	reply.Success = true
}

func (rf *Raft) getEntry(index int) *LogEntry {
	logEntries := rf.log
	//假设logEntries==[1,2,3,4,5],lastIncludedIndex==3,index==4,那么logIndex==1
	logIndex := index - rf.lastIncludedIndex
	if logIndex < 0 {
		log.Panicf("LogEntries.getEntry: index too small. (%d < %d)", index, rf.lastIncludedIndex)
	}
	//如果给定的索引刚好是最后被快照的日志条目的下标索引,那么任期也应该是最后被快照的日志条目的下标任期
	if logIndex == 0 {
		return &LogEntry{
			Command: nil,
			Term:    rf.lastIncludedTerm,
		}
	}
	//// 如果计算得到的索引超过当前节点日志的长度，返回一个特殊标志，表示没有对应的日志条目
	if logIndex > len(logEntries) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &logEntries[logIndex-1]
}

// Get the index and term of the last entry.
// Return (0, 0) if the log is empty.
func (rf *Raft) lastLogInfo() (index, term int) {
	logEntries := rf.log
	//假设logEntries==[1,2,3,4,5],lastIncludedIndex==3,那么有效logEntries==[4,5],则len(logEntries)==2
	index = len(logEntries) + rf.lastIncludedIndex
	logEntry := rf.getEntry(index)
	return index, logEntry.Term
}

// Get the slice of the log with index from startIndex to endIndex.
func (rf *Raft) getSlice(startIndex, endIndex int) []LogEntry {
	logEntries := rf.log
	logStartIndex := startIndex - rf.lastIncludedIndex
	logEndIndex := endIndex - rf.lastIncludedIndex
	if logStartIndex <= 0 {
		Debug(dError, "LogEntries.getSlice: startIndex out of range. startIndex: %d, len: %d.",
			startIndex, len(logEntries))
		log.Panicf("LogEntries.getSlice: startIndex out of range. (%d < %d)", startIndex, rf.lastIncludedIndex)
	}
	if logEndIndex > len(logEntries)+1 {
		Debug(dError, "LogEntries.getSlice: endIndex out of range. endIndex: %d, len: %d.",
			endIndex, len(logEntries))
		log.Panicf("LogEntries.getSlice: endIndex out of range. (%d > %d)", endIndex, len(logEntries)+1+rf.lastIncludedIndex)
	}
	if logStartIndex > logEndIndex {
		Debug(dError, "LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
		log.Panicf("LogEntries.getSlice: startIndex > endIndex. (%d > %d)", startIndex, endIndex)
	}
	return append([]LogEntry(nil), logEntries[logStartIndex-1:logEndIndex-1]...)
}

// Get the index of first entry and last entry with the given term.
// Return (-1,-1) if no such term is found
func (rf *Raft) getBoundsWithTerm(term int) (minIndex int, maxIndex int) {
	logEntries := rf.log
	if term == 0 {
		return 0, 0
	}
	minIndex, maxIndex = math.MaxInt, -1
	for i := rf.lastIncludedIndex + 1; i <= rf.lastIncludedIndex+len(logEntries); i++ {
		if rf.getEntry(i).Term == term {
			minIndex = min(minIndex, i)
			maxIndex = max(maxIndex, i)
		}
	}
	if maxIndex == -1 {
		return -1, -1
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

/*------------------2C:持久化------------------------------------------*/
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	//根据论文图 2 中的描述，需要被持久化的变量主要有日志条目 log，节点当前任期 currentTerm，以及节点目前投给了谁 votedFor
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.currentTerm\". err: %v, data: %v", err, rf.currentTerm)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.votedFor\". err: %v, data: %v", err, rf.votedFor)
	}
	if err := e.Encode(rf.log); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.log\". err: %v, data: %v", err, rf.log)
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedIndex\". err: %v, data: %v", err, rf.lastIncludedIndex)
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedTerm\". err: %v, data: %v", err, rf.lastIncludedTerm)
	}
	data := w.Bytes()
	// leave the second parameter nil, will use it in PartD
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if err := d.Decode(&rf.currentTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.currentTerm\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.votedFor\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.log); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.log\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.lastIncludedIndex\". err: %v, data: %s", err, data)
	}
	if err := d.Decode(&rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.readPersist: failed to decode \"rf.lastIncludedTerm\". err: %v, data: %s", err, data)
	}
}

// save Raft's persistent state and service snapshot to stable storage,
// where they can later be retrieved after a crash and restart.
func (rf *Raft) persistAndSnapshot(snapshot []byte) {
	Debug(dSnap, "S%d Saving persistent state and service snapshot to stable storage at T%d.", rf.me, rf.currentTerm)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.currentTerm\". err: %v, data: %v", err, rf.currentTerm)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.votedFor\". err: %v, data: %v", err, rf.votedFor)
	}
	if err := e.Encode(rf.log); err != nil {
		Debug(dError, "Raft.persistAndSnapshot: failed to encode \"rf.log\". err: %v, data: %v", err, rf.log)
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedIndex\". err: %v, data: %v", err, rf.lastIncludedIndex)
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		Debug(dError, "Raft.persist: failed to encode \"rf.lastIncludedTerm\". err: %v, data: %v", err, rf.lastIncludedTerm)
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

/*----------------------日志压缩-----------------------------------------*/
// CondInstallSnapshot returns if the service wants to switch to snapshot.
// Only do so if Raft hasn't had more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d Installing the snapshot. LLI: %d, LLT: %d", rf.me, lastIncludedIndex, lastIncludedTerm)
	lastLogIndex, _ := rf.lastLogInfo()
	if rf.commitIndex >= lastIncludedIndex {
		Debug(dSnap, "S%d Log entries is already up-to-date with the snapshot. (%d >= %d)", rf.me, rf.commitIndex, lastIncludedIndex)
		return false
	}
	if lastLogIndex >= lastIncludedIndex {
		rf.log = rf.getSlice(lastIncludedIndex+1, lastLogIndex+1)
	} else {
		rf.log = []LogEntry{}
	}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.snapshot = snapshot
	rf.persistAndSnapshot(snapshot)
	return true
}

// Raft 协议当前状态机已经应用了多少条日志，然后它打算进行一轮快照（snapshot）来压缩日志
// 参数：预期截断的下标，以及被压缩后的快照信息
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	Debug(dSnap, "S%d Snapshotting through index %d.", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex, _ := rf.lastLogInfo()
	//当前节点已经应用了一个包含传入索引的快照，无需再次进行快照
	if rf.lastIncludedIndex >= index {
		Debug(dSnap, "S%d Snapshot already applied to persistent storage. (%d >= %d)", rf.me, rf.lastIncludedIndex, index)
		return
	}
	//在 index 之前的日志还未提交，不能安全地截断这些未提交的日志
	if rf.commitIndex < index {
		Debug(dWarn, "S%d Cannot snapshot uncommitted log entries, discard the call. (%d < %d)", rf.me, rf.commitIndex, index)
		return
	}
	newLog := rf.getSlice(index+1, lastLogIndex+1) //slice是左闭右开
	newLastIncludeTerm := rf.getEntry(index).Term

	// 更新 Raft 节点的相关状态
	rf.lastIncludedTerm = newLastIncludeTerm
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot

	// 持久化快照信息
	rf.persistAndSnapshot(snapshot)

}

func (rf *Raft) sendSnapshot(server int) {
	Debug(dSnap, "S%d -> S%d Sending installing snapshot request at T%d.", rf.me, server, rf.currentTerm)
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	go rf.leaderSendSnapshot(args, server)
}

func (rf *Raft) leaderSendSnapshot(args *InstallSnapshotArgs, server int) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dSnap, "S%d <- S%d Received install snapshot reply at T%d.", rf.me, server, rf.currentTerm)
		if reply.Term < rf.currentTerm {
			Debug(dLog, "S%d Term lower, invalid install snapshot reply. (%d < %d)",
				rf.me, reply.Term, rf.currentTerm)
			return
		}
		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the install snapshot request, install snapshot reply discarded. "+
				"requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
			return
		}
		rf.checkTerm(reply.Term)
		newNext := args.LastIncludedIndex + 1
		newMatch := args.LastIncludedIndex
		rf.matchIndex[server] = max(newMatch, rf.matchIndex[server])
		rf.nextIndex[server] = max(newNext, rf.nextIndex[server])
	}
}

// InstallSnapshot sends an apply message with the snapshot to applyCh.
// The state machine should cooperate with raft code later to decide whether to install the snapshot using CondInstallSnapshot function.
// No snapshot related status in raft code should be changed right now,
// which could result in inconsistency between the status machine and raft code, as the snapshot is not applied immediately.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d <- S%d Received install snapshot request at T%d.", rf.me, args.LeaderId, rf.currentTerm)

	//1.Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		Debug(dSnap, "S%d Term is lower, rejecting install snapshot request. (%d < %d)", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	Debug(dTimer, "S%d Resetting ELT, wait for next potential heartbeat timeout.", rf.me)
	rf.lastHeartbeat = time.Now()

	// 所有的应用消息都应该在一个单独的 goroutine 中发送（applyLogsLoop 函数），
	// 否则可能导致应用动作出现乱序，其中快照应用可能在命令应用正在运行时插队。
	if rf.waitingIndex >= args.LastIncludedIndex {
		Debug(dSnap, "S%d A newer snapshot already exists, rejecting install snapshot request. (%d <= %d)",
			rf.me, args.LastIncludedIndex, rf.waitingIndex)
		return
	}
	rf.waitingSnapshot = args.Data
	rf.waitingIndex = args.LastIncludedIndex
	rf.waitingTerm = args.LastIncludedTerm
}
