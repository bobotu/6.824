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

type state int

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
	leader      int
	currentTerm int
	votedFor    int
	logs        []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
	rpcCh   chan rpcCall

	state state

	close chan struct{}
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

	for rf.state == follower {
		select {
		case <-timer.C:
			rf.state = candidate

		case req := <-rf.rpcCh:
			switch cmd := req.command.(type) {
			case AttachLogsArgs:
				// log.Printf("follower rpc: %d follower got an attach rpc call\n", rf.me)
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
			}
		case <-rf.close:
			return
		}
	}
}

func (rf *Raft) runCandidate() {
	log.Printf("state candidate: %d run into candidate\n", rf.me)

	for rf.state == candidate {
		rf.mu.Lock()
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		timer := time.NewTimer(randDuration())
		count := rf.requestVotes()
		rf.mu.Unlock()
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
				rf.state = leader
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
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.logs) - 1
	rf.mu.Unlock()

	rf.leader = rf.me
	ticker := time.NewTicker(50 * time.Millisecond)

	for rf.state == leader {
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
			}
		case <-rf.close:
			return
		}
	}
}

func (rf *Raft) leaderCommitLog() {
	if rf.state != leader {
		return
	}

	rf.mu.Lock()
	l := len(rf.logs)
	rf.mu.Unlock()

	for next := l; next > rf.commitIndex; next-- {
		count := 1

		for i, match := range rf.matchIndex {
			if i != rf.me && match >= next {
				count++
			}
		}

		if count >= len(rf.peers)/2+1 && next < len(rf.logs) && rf.logs[next].Term == rf.currentTerm {
			rf.commitIndex = next
			break
		}
	}

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

		switch rf.state {
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
	li, lt := rf.logState()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
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

				if resp.Term > rf.currentTerm {
					rf.state = follower
					return
				}

				if resp.VoteGranted {
					atomic.AddInt32(&count, 1)
				} else if resp.Term < rf.currentTerm {
					if int(atomic.AddInt32(&fastCount, 1)) >= len(rf.peers)/2+1 {
						rf.state = follower
						rf.currentTerm = -1
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

func (rf *Raft) sendEntries(peer int) {
	for {
		rf.mu.Lock()

		nextLogIndex := rf.nextIndex[peer]
		prevLogIndex := nextLogIndex - 1
		prevLogTerm := 0
		if prevLogIndex >= 0 && len(rf.logs) > prevLogIndex {
			prevLogTerm = rf.logs[prevLogIndex].Term
		}
		entries := rf.logs[nextLogIndex:]

		args := AttachLogsArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		rf.mu.Unlock()

		//if len(entries) > 0 {
		//	log.Printf("attach: %d start attach logs to %d", rf.me, peer)
		//}

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

		rf.mu.Lock()
		if resp.Term > rf.currentTerm {
			rf.state = follower
			rf.mu.Unlock()
			return
		}

		if resp.Term == args.Term {
			rf.nextIndex[peer] = resp.NextIndex
			if resp.NextIndex > len(rf.logs) {
				rf.nextIndex[peer] = len(rf.logs)
			}
		}
		rf.mu.Unlock()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	term := rf.currentTerm
	isleader := rf.leader == rf.me
	// Your code here.
	return term, isleader
}

func (rf *Raft) logState() (lastLogIndex int, lastLogTerm int) {
	if len(rf.logs) == 0 {
		lastLogIndex = -1
		lastLogTerm = 0
	} else {
		lastLogIndex = len(rf.logs) - 1
		lastLogTerm = rf.logs[lastLogIndex].Term
	}

	return
}

func randDuration() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}

type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Logs        []Log
	State       state
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	p := persistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Logs:        rf.logs,
		State:       rf.state,
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
	rf.currentTerm = p.CurrentTerm
	rf.votedFor = p.VotedFor
	rf.logs = p.Logs
	rf.state = p.State
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
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		log.Printf("vote: %d reject %d, beacuse term %d less than %d\n", rf.me, args.LastLogIndex, args.Term, rf.currentTerm)
		valid = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.state = follower
		rf.currentTerm = args.Term
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
	case <-time.After(20 * time.Millisecond):
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
	if args.Term < rf.currentTerm {
		log.Printf("attach: %d get attach logs req form %d which term less than itself, reject", rf.me, args.LeaderID)
		reply.Success = false
		reply.Term = rf.currentTerm
		valid = false
		return
	}

	valid = true
	rf.leader = args.LeaderID

	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.state = follower
		rf.persist()
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm

	if len(rf.logs) <= args.PrevLogIndex {
		reply.Success = false
		reply.NextIndex = len(rf.logs)
		return
	}

	if args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		termAtIndex := rf.logs[args.PrevLogIndex].Term
		var i int
		for i = args.PrevLogIndex; i >= 0; i-- {
			if rf.logs[i].Term != termAtIndex {
				break
			}
		}
		reply.NextIndex = i + 1
		return
	}

	if args.PrevLogIndex < 0 {
		rf.logs = args.Entries
	} else if len(args.Entries) > 0 {
		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
		//log.Printf("attach: follower %d attached, now logs are %v\n", rf.me, rf.logs)
	}

	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.logs)-1 < args.LeaderCommit {
			rf.commitIndex = len(rf.logs) - 1
		}
	}

	rf.applyLogs()

	reply.Success = true

	return
}

func (rf *Raft) applyLogs() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++

		//log.Printf("commit: %d apply %d\n", rf.me, rf.commitIndex)

		rf.applyCh <- ApplyMsg{
			Command: rf.logs[rf.lastApplied].Data,
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
	case <-time.After(20 * time.Millisecond):
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

	isLeader = rf.state == leader
	if !isLeader {
		return
	}

	term = rf.currentTerm
	index = len(rf.logs)
	rf.logs = append(rf.logs, Log{
		Term: term,
		Data: command,
	})

	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.persist()

	log.Printf("append log: new log %v\n", rf.logs)

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
		state:       follower,
		votedFor:    -1,
		logs:        make([]Log, 0, 10),
		applyCh:     applyCh,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		rpcCh:       make(chan rpcCall, 10),
		leader:      -1,
		commitIndex: -1,
		lastApplied: -1,
		close:       make(chan struct{}),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.mainLoop()

	return rf
}
