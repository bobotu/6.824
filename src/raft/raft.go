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
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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

type Log struct {
	Term int
	Data interface{}
}

const (
	follower = iota
	leader
	candidate
)

type rpcCall struct {
	command interface{}
	reply   chan<- interface{}
}

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
	leader      atomic.Value
	currentTerm atomic.Value
	votedFor    int
	logs        []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
	rpcCh   chan rpcCall

	state atomic.Value

	startLogIndex int
	startLogTerm  int

	close chan struct{}
}

func (rf *Raft) logsLen() int {
	return len(rf.logs) + rf.startLogIndex
}

func (rf *Raft) logAt(i int) Log {
	return rf.logs[i-rf.startLogIndex]
}

func (rf *Raft) runFollower() {
	log.Printf("state follower: %d run into follower\n", rf.me)
	timeout := randDuration()
	timer := time.NewTimer(timeout)

	reset := func() {
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(timeout)
	}

	for rf.state.Load() == follower {
		select {
		case <-timer.C:
			rf.state.Store(candidate)

		case req := <-rf.rpcCh:
			switch cmd := req.command.(type) {
			case AttachLogsArgs:
				//log.Printf("follower rpc: %d follower got an attach rpc call\n", rf.me)
				r, valid := rf.handleAttachLogs(cmd)
				req.reply <- r
				if valid {
					reset()
				}
			case RequestVoteArgs:
				//log.Printf("follower rpc: %d follower got a vote rpc call\n", rf.me)
				r, valid := rf.handleRequestVote(cmd)
				req.reply <- r
				if valid && rf.votedFor != -1 {
					reset()
				}
			case InstallSnapshotArgs:
				r, valid := rf.handleInstallSnapshot(cmd)
				req.reply <- r
				if valid && rf.votedFor != -1 {
					reset()
				}
			}
		case <-rf.close:
			return
		}
	}
}

func (rf *Raft) runCandidate() {
	log.Printf("state candidate: %d run into candidate\n", rf.me)

	for rf.state.Load() == candidate {
		rf.mu.Lock()
		rf.currentTerm.Store(rf.currentTerm.Load().(int) + 1)
		rf.votedFor = rf.me
		rf.persist()
		timer := time.NewTimer(randDuration())
		rf.mu.Unlock()
		count := rf.requestVotes()

	selectCh:
		select {
		case <-timer.C:
			log.Printf("vote: %d election timeout\n", rf.me)

		case votes := <-count:
			log.Printf("vote: %d get %d votes\n", rf.me, votes)
			if votes < len(rf.peers)/2+1 {
				log.Printf("vote: %d election votes not enough\n", rf.me)
				goto selectCh
			} else {
				log.Printf("vote: %d become leader\n", rf.me)
				rf.state.Store(leader)
			}

		case req := <-rf.rpcCh:
			var valid bool
			switch cmd := req.command.(type) {
			case AttachLogsArgs:
				var r AttachLogsReply
				r, valid = rf.handleAttachLogs(cmd)
				req.reply <- r
			case RequestVoteArgs:
				var r RequestVoteReply
				r, valid = rf.handleRequestVote(cmd)
				req.reply <- r
			case InstallSnapshotArgs:
				var r InstallSnapshotReply
				r, valid = rf.handleInstallSnapshot(cmd)
				req.reply <- r
			}
			if valid {
				continue
			} else {
				goto selectCh
			}

		case <-rf.close:
			return
		}
	}
}

func (rf *Raft) runLeader() {
	log.Printf("state leader: %d run into leader\n", rf.me)

	rf.mu.Lock()
	for i := range rf.peers {
		rf.nextIndex[i] = rf.logsLen()
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.logsLen() - 1 + rf.startLogIndex
	rf.mu.Unlock()

	rf.leader.Store(rf.me)
	ticker := time.NewTicker(50 * time.Millisecond)

	for rf.state.Load() == leader {
		select {
		case <-ticker.C:
			var wg sync.WaitGroup
			for i := range rf.peers {
				if i != rf.me {
					wg.Add(1)
					go func(i int) {
						rf.sendEntries(i)
						wg.Done()
					}(i)
				}
			}
			wg.Wait()
			rf.leaderCommitLog()
		case req := <-rf.rpcCh:
			switch cmd := req.command.(type) {
			case AttachLogsArgs:
				r, _ := rf.handleAttachLogs(cmd)
				req.reply <- r
			case RequestVoteArgs:
				r, _ := rf.handleRequestVote(cmd)
				req.reply <- r
			case InstallSnapshotArgs:
				r, _ := rf.handleInstallSnapshot(cmd)
				req.reply <- r
			}
		case <-rf.close:
			return
		}
	}
}

func (rf *Raft) leaderCommitLog() {
	if rf.state.Load() != leader {
		return
	}

	rf.mu.Lock()
	l := rf.logsLen()
	for next := l; next > rf.commitIndex; next-- {
		count := 1

		for i, match := range rf.matchIndex {
			if i != rf.me && match >= next {
				count++
			}
		}

		if count >= len(rf.peers)/2+1 && next < rf.logsLen() && rf.logAt(next).Term == rf.currentTerm.Load() {
			rf.commitIndex = next
			break
		}
	}
	rf.mu.Unlock()

	rf.applyLogs()

	//log.Printf("commit: leader %d commitIndex set to %d, %v\n", rf.me, rf.commitIndex, rf.matchIndex)
}

func (rf *Raft) mainLoop() {
	for {
		select {
		case <-rf.close:
			return
		default:
		}

		switch rf.state.Load().(int) {
		case follower:
			rf.runFollower()
		case candidate:
			rf.runCandidate()
		case leader:
			rf.runLeader()
		}
	}
}

func (rf *Raft) requestVotes() <-chan int {
	rf.mu.Lock()
	li, lt := rf.logState()
	rf.mu.Unlock()

	args := RequestVoteArgs{
		Term:         rf.currentTerm.Load().(int),
		CandidateID:  rf.me,
		LastLogIndex: li,
		LastLogTerm:  lt,
	}
	var count int32 = 1
	var fastCount int32
	var wg sync.WaitGroup
	ch := make(chan int)

	for i := range rf.peers {
		if i != rf.me {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				resp := &RequestVoteReply{}
				log.Printf("vote: %d ask %d for vote\n", rf.me, i)

				if !rf.sendRequestVote(i, args, resp) {
					return
				}

				if resp.Term > rf.currentTerm.Load().(int) {
					rf.state.Store(follower)
					return
				}

				if resp.VoteGranted {
					atomic.AddInt32(&count, 1)
				} else if resp.Term < rf.currentTerm.Load().(int) {
					if int(atomic.AddInt32(&fastCount, 1)) >= len(rf.peers)/2+1 {
						rf.state.Store(follower)
						rf.currentTerm.Store(-1)
						log.Printf("vote: %d may suffered network split, return to follower, term -1\n", rf.me)
					}
				}
			}(i)
		}
	}

	go func() {
		wg.Wait()
		log.Printf("vote: %d request votes done\n", rf.me)
		votes := int(atomic.LoadInt32(&count))
		ch <- votes
	}()

	return ch
}

func (rf *Raft) installSnapshotTo(peer int) bool {
	log.Printf("snap: %d sending install req to %d me log len %d\n", rf.me, peer, rf.logsLen())
	args := InstallSnapshotArgs{
		Term:             rf.currentTerm.Load().(int),
		Data:             rf.persister.ReadSnapshot(),
		LastIndexInclude: rf.startLogIndex - 1,
		LastTermInclude:  rf.startLogTerm,
		LeaderID:         rf.me,
	}
	var reply InstallSnapshotReply
	if rf.sendInstallSnapshot(peer, args, &reply) {
		if reply.Term > rf.currentTerm.Load().(int) {
			rf.state.Store(follower)
			return false
		}

		rf.nextIndex[peer] = args.LastIndexInclude + 1
		rf.matchIndex[peer] = args.LastIndexInclude

		return true
	} else {
		return false
	}
}

func (rf *Raft) sendEntries(peer int) {
	for {
		rf.mu.Lock()

		nextLogIndex := rf.nextIndex[peer]
		prevLogIndex := nextLogIndex - 1
		prevLogTerm := 0

		if prevLogIndex >= 0 && rf.logsLen() > prevLogIndex {
			if nextLogIndex > rf.startLogIndex {
				prevLogTerm = rf.logAt(prevLogIndex).Term
			} else if nextLogIndex == rf.startLogIndex {
				prevLogTerm = rf.startLogTerm
			} else {
				if rf.installSnapshotTo(peer) {
					prevLogIndex = -1
					nextLogIndex = rf.startLogIndex
				} else {
					rf.mu.Unlock()
					return
				}
			}
		}

		entries := rf.logs[nextLogIndex-rf.startLogIndex:]

		args := AttachLogsArgs{
			Term:         rf.currentTerm.Load().(int),
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		rf.mu.Unlock()

		var resp AttachLogsReply
		if !rf.sendAttachLogs(peer, args, &resp) {
			return
		}

		if resp.Success {
			rf.mu.Lock()
			rf.nextIndex[peer] += len(args.Entries)
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
			rf.mu.Unlock()
			return
		}

		if resp.Term > rf.currentTerm.Load().(int) {
			rf.state.Store(follower)
			return
		}

		rf.mu.Lock()
		if resp.Term == args.Term {
			rf.nextIndex[peer] = resp.NextIndex
			if resp.NextIndex > rf.logsLen() {
				rf.nextIndex[peer] = rf.logsLen()
			}
		}
		rf.mu.Unlock()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isleader bool) {
	term = rf.currentTerm.Load().(int)
	isleader = rf.state.Load().(int) == leader
	return
}

func (rf *Raft) logState() (lastLogIndex int, lastLogTerm int) {
	if rf.logsLen() == 0 {
		lastLogIndex = -1
		lastLogTerm = 0
	} else {
		lastLogIndex = rf.logsLen() - 1
		if lastLogIndex < rf.startLogIndex {
			lastLogTerm = rf.startLogTerm
		} else {
			lastLogTerm = rf.logAt(lastLogIndex).Term
		}
	}

	return
}

func randDuration() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}

type persistentState struct {
	CurrentTerm   int
	VotedFor      int
	Logs          []Log
	State         int
	StartLogIndex int
	StartLogTerm  int
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	p := persistentState{
		CurrentTerm:   rf.currentTerm.Load().(int),
		VotedFor:      rf.votedFor,
		Logs:          rf.logs,
		State:         rf.state.Load().(int),
		StartLogIndex: rf.startLogIndex,
		StartLogTerm:  rf.startLogTerm,
	}
	buf := bytes.NewBuffer(nil)
	gob.NewEncoder(buf).Encode(p)
	rf.persister.SaveRaftState(buf.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	var p persistentState
	buf := bytes.NewReader(data)
	gob.NewDecoder(buf).Decode(&p)
	rf.currentTerm.Store(p.CurrentTerm)
	rf.votedFor = p.VotedFor
	rf.logs = p.Logs
	rf.state.Store(p.State)
	rf.startLogIndex = p.StartLogIndex
	rf.startLogTerm = p.StartLogTerm
	rf.lastApplied = p.StartLogIndex - 1
	rf.commitIndex = p.StartLogIndex - 1
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) handleRequestVote(args RequestVoteArgs) (reply RequestVoteReply, valid bool) {
	reply.Term = rf.currentTerm.Load().(int)
	reply.VoteGranted = false

	if args.Term < rf.currentTerm.Load().(int) {
		log.Printf("vote: %d reject %d, beacuse term %d less than %d\n", rf.me, args.CandidateID, args.Term, rf.currentTerm.Load())
		valid = false
		return
	}

	if args.Term > rf.currentTerm.Load().(int) {
		rf.mu.Lock()
		rf.state.Store(follower)
		rf.currentTerm.Store(args.Term)
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		log.Printf("vote: %d reject %d, beacuse it had voted for someone\n", rf.me, args.CandidateID)
		return
	}

	valid = true

	rf.mu.Lock()
	lastLogIndex, lastLogTerm := rf.logState()
	rf.mu.Unlock()

	if lastLogTerm > args.LastLogTerm {
		log.Printf("vote: %d reject %d, because lastLogTerm %d less than %d\n", rf.me, args.CandidateID, args.LastLogTerm, lastLogTerm)
		return
	}

	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		log.Printf("vote: %d reject %d, because lastLogIndex %d less than %d\n", rf.me, args.CandidateID, args.LastLogIndex, lastLogIndex)
		return
	}

	rf.mu.Lock()
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	rf.persist()
	rf.mu.Unlock()

	return
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	resp := make(chan interface{})
	rf.rpcCh <- rpcCall{command: args, reply: resp}

	*reply = (<-resp).(RequestVoteReply)
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
	ch := make(chan bool)
	go func() {
		ch <- rf.peers[server].Call("Raft.RequestVote", args, reply)
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		return false
	case r := <-ch:
		return r
	}
}

type AttachLogsArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AttachLogsReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) handleAttachLogs(args AttachLogsArgs) (reply AttachLogsReply, valid bool) {
	if args.Term < rf.currentTerm.Load().(int) {
		log.Printf("attach: %d get attach logs req form %d which term less than itself, reject", rf.me, args.LeaderID)
		reply.Success = false
		reply.Term = rf.currentTerm.Load().(int)
		valid = false
		return
	}

	valid = true
	rf.leader.Store(args.LeaderID)

	if args.Term > rf.currentTerm.Load().(int) {
		rf.mu.Lock()
		rf.currentTerm.Store(args.Term)
		rf.state.Store(follower)
		rf.persist()
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm.Load().(int)
	reply.NextIndex = rf.startLogIndex

	if rf.logsLen() <= args.PrevLogIndex {
		reply.Success = false
		reply.NextIndex = rf.logsLen()
		return
	}

	if args.PrevLogIndex > 0 && args.PrevLogIndex < rf.startLogIndex-1 {
		reply.Success = false
		reply.NextIndex = rf.startLogIndex

		return
	}

	if args.PrevLogIndex > 0 && args.PrevLogIndex >= rf.startLogIndex && rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		termAtIndex := rf.logAt(args.PrevLogIndex).Term
		var i int
		for i = args.PrevLogIndex; i >= rf.startLogIndex; i-- {
			if rf.logAt(i).Term != termAtIndex {
				break
			}
		}
		reply.NextIndex = i + 1
		return
	}

	if args.PrevLogIndex < 0 {
		rf.logs = args.Entries
	} else if len(args.Entries) > 0 {
		rf.logs = append(rf.logs[:args.PrevLogIndex-rf.startLogIndex+1], args.Entries...)
	}

	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.logsLen()-1 < args.LeaderCommit {
			rf.commitIndex = rf.logsLen() - 1
		}
	}

	rf.applyLogs()

	reply.Success = true

	return
}

func (rf *Raft) applyLogs() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++

		rf.mu.Lock()
		cmd := rf.logAt(rf.lastApplied).Data
		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{
			Command: cmd,
			Index:   rf.lastApplied + 1,
		}
	}
}

func (rf *Raft) AttachLogs(args AttachLogsArgs, reply *AttachLogsReply) {
	resp := make(chan interface{})
	rf.rpcCh <- rpcCall{args, resp}
	*reply = (<-resp).(AttachLogsReply)
}

func (rf *Raft) sendAttachLogs(server int, args AttachLogsArgs, reply *AttachLogsReply) bool {
	ch := make(chan bool)
	go func() {
		ch <- rf.peers[server].Call("Raft.AttachLogs", args, reply)
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		return false
	case r := <-ch:
		return r
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.state.Load() == leader
	if !isLeader {
		return
	}

	term = rf.currentTerm.Load().(int)
	index = rf.logsLen()
	rf.logs = append(rf.logs, Log{
		Term: term,
		Data: command,
	})

	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.persist()

	// log.Printf("append log: new log %v\n", rf.logs)

	return index + 1, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.close)
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
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		votedFor:    -1,
		logs:        make([]Log, 0, 10),
		applyCh:     applyCh,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		rpcCh:       make(chan rpcCall, 10),
		commitIndex: -1,
		lastApplied: -1,
		close:       make(chan struct{}),
	}
	// initialize from state persisted before a crash
	rf.state.Store(follower)
	rf.leader.Store(-1)
	rf.currentTerm.Store(0)
	rf.readPersist(persister.ReadRaftState())
	if snap := rf.persister.ReadSnapshot(); snap != nil {
		rf.applyCh <- ApplyMsg{
			UseSnapshot: true,
			Snapshot:    snap,
		}
	}
	go rf.mainLoop()

	return rf
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderID         int
	LastIndexInclude int
	LastTermInclude  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	resp := make(chan interface{})
	rf.rpcCh <- rpcCall{args, resp}
	*reply = (<-resp).(InstallSnapshotReply)
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ch := make(chan bool)
	go func() {
		ch <- rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		return false
	case r := <-ch:
		return r
	}
}

func (rf *Raft) handleInstallSnapshot(args InstallSnapshotArgs) (reply InstallSnapshotReply, valid bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("snap: %d get req form %d, index: %d, my log len %d\n", rf.me, args.LeaderID, args.LastIndexInclude, rf.logsLen())
	reply.Term = rf.currentTerm.Load().(int)

	if args.LastIndexInclude < rf.startLogIndex {
		valid = true
		return
	}

	if args.Term < rf.currentTerm.Load().(int) {
		valid = false
		return
	}

	rf.persister.SaveSnapshot(args.Data)

	if rf.logsLen() > args.LastIndexInclude && rf.logAt(args.LastIndexInclude).Term == args.LastTermInclude {
		rf.logs = rf.logs[args.LastIndexInclude-rf.startLogIndex+1:]
	} else {
		rf.logs = []Log{}
	}
	rf.startLogIndex = args.LastIndexInclude + 1
	rf.startLogTerm = args.LastTermInclude
	rf.leader.Store(args.LeaderID)
	rf.lastApplied = args.LastIndexInclude

	rf.persist()

	rf.applyCh <- ApplyMsg{
		UseSnapshot: true,
		Snapshot:    args.Data,
	}

	//log.Printf("snap: %d installed\n", rf.me)

	valid = true
	return
}

type Snapshot struct {
	Data  []byte
	Index int
}

func (rf *Raft) SaveSnapshot(snap Snapshot) {
	rf.mu.Lock()
	if snap.Index < rf.startLogIndex {
		rf.mu.Unlock()
		return
	}
	rf.persister.SaveSnapshot(snap.Data)

	args := InstallSnapshotArgs{
		Term:             rf.currentTerm.Load().(int),
		LeaderID:         rf.me,
		LastIndexInclude: snap.Index - 1,
		LastTermInclude:  rf.startLogTerm,
		Data:             snap.Data,
	}

	rf.nextIndex[rf.me] = args.LastIndexInclude + 1
	rf.matchIndex[rf.me] = args.LastIndexInclude
	rf.startLogTerm = rf.logAt(snap.Index - 1).Term
	rf.logs = rf.logs[snap.Index-rf.startLogIndex:]
	rf.startLogIndex = snap.Index

	rf.persist()

	if rf.state.Load() == leader {
		var wg sync.WaitGroup
		for i, next := range rf.nextIndex {
			if next < snap.Index && i != rf.me {
				wg.Add(1)
				go func(peer int) {
					defer wg.Done()
					var reply InstallSnapshotReply
					if rf.sendInstallSnapshot(peer, args, &reply) {
						if reply.Term > rf.currentTerm.Load().(int) {
							rf.state.Store(follower)
							return
						}
						rf.mu.Lock()
						rf.nextIndex[peer] = args.LastIndexInclude + 1
						rf.matchIndex[peer] = args.LastIndexInclude
						rf.mu.Unlock()
					} else {
						//log.Printf("snap: %d install faild\n", peer)
					}
				}(i)
			}
		}
		rf.mu.Unlock()
		wg.Wait()
	} else {
		rf.mu.Unlock()
	}
}
