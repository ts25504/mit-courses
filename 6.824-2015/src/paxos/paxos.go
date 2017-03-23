package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type InstanceInfo struct {
	np     int64
	na     int64
	va     interface{}
	status Fate
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances map[int]*InstanceInfo
	maxdone   []int
}

const (
	Ok     = "Ok"
	Reject = "Reject"
)

type PrepareArgs struct {
	Seq int
	Num int64
}

type PrepareReply struct {
	Err   string
	Num   int64
	Value interface{}
}

type AcceptArgs struct {
	Seq   int
	Num   int64
	Value interface{}
}

type AcceptReply struct {
	Err string
}

type DecidedArgs struct {
	Seq   int
	Num   int64
	Value interface{}
	Me    int
	Done  int
}

type DecidedReply struct {
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) makeInstanceInfo() *InstanceInfo {
	info := &InstanceInfo{}
	info.np = 0
	info.na = 0
	info.va = nil
	info.status = Pending
	return info
}

func (px *Paxos) getNum() int64 {
	now := time.Now()
	return now.UnixNano()
}

func (px *Paxos) doPrepare(args *PrepareArgs, reply *PrepareReply, info *InstanceInfo) {
	info.np = args.Num
	reply.Num = info.na
	reply.Value = info.va
	reply.Err = Ok
}

func (px *Paxos) rejectPrepare(reply *PrepareReply) {
	reply.Err = Reject
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	_, ok := px.instances[args.Seq]
	if !ok {
		px.instances[args.Seq] = px.makeInstanceInfo()
		px.doPrepare(args, reply, px.instances[args.Seq])
	} else {
		if args.Num > px.instances[args.Seq].np {
			px.doPrepare(args, reply, px.instances[args.Seq])
		} else {
			px.rejectPrepare(reply)
		}
	}

	return nil
}

func (px *Paxos) sendPrepare(seq int, v interface{}, num int64) (bool, interface{}) {
	args := &PrepareArgs{}
	args.Seq = seq
	args.Num = num
	acceptCount := 0
	var acceptNum int64 = 0
	acceptValue := v

	for i, peer := range px.peers {
		reply := &PrepareReply{}
		reply.Num = 0
		reply.Value = nil
		reply.Err = Reject
		if i == px.me {
			px.Prepare(args, reply)
		} else {
			call(peer, "Paxos.Prepare", args, reply)
		}

		if reply.Err == Ok {
			acceptCount += 1
			if reply.Num > acceptNum {
				acceptNum = reply.Num
				acceptValue = reply.Value
			}
		}
	}

	if acceptCount >= len(px.peers)/2+1 {
		return true, acceptValue
	}

	return false, acceptValue
}

func (px *Paxos) doAccept(args *AcceptArgs, reply *AcceptReply, info *InstanceInfo) {
	info.np = args.Num
	info.na = args.Num
	info.va = args.Value
	reply.Err = Ok
}

func (px *Paxos) rejectAccept(reply *AcceptReply) {
	reply.Err = Reject
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	_, ok := px.instances[args.Seq]
	if !ok {
		px.instances[args.Seq] = px.makeInstanceInfo()
		px.doAccept(args, reply, px.instances[args.Seq])
	} else {
		if args.Num >= px.instances[args.Seq].np {
			px.doAccept(args, reply, px.instances[args.Seq])
		} else {
			px.rejectAccept(reply)
		}
	}

	return nil
}

func (px *Paxos) sendAccept(seq int, v interface{}, num int64) bool {
	args := &AcceptArgs{}
	args.Seq = seq
	args.Num = num
	args.Value = v
	acceptCount := 0

	for i, peer := range px.peers {
		reply := &AcceptReply{}
		reply.Err = Reject
		if i == px.me {
			px.Accept(args, reply)
		} else {
			call(peer, "Paxos.Accept", args, reply)
		}

		if reply.Err == Ok {
			acceptCount += 1
		}
	}

	if acceptCount >= len(px.peers)/2+1 {
		return true
	}

	return false
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	_, ok := px.instances[args.Seq]
	if !ok {
		px.instances[args.Seq] = px.makeInstanceInfo()
	}
	px.instances[args.Seq].na = args.Num
	px.instances[args.Seq].np = args.Num
	px.instances[args.Seq].va = args.Value
	px.instances[args.Seq].status = Decided

	px.maxdone[args.Me] = args.Done

	return nil
}

func (px *Paxos) sendDecided(seq int, v interface{}, num int64) {
	args := &DecidedArgs{}
	args.Seq = seq
	args.Value = v
	args.Done = px.maxdone[px.me]
	args.Me = px.me
	args.Num = num

	for i, peer := range px.peers {
		var reply DecidedReply
		if i == px.me {
			px.Decided(args, &reply)
		} else {
			call(peer, "Paxos.Decided", args, &reply)
		}
	}
}

func (px *Paxos) proposer(seq int, v interface{}) {
	for {
		num := px.getNum()
		ok, va := px.sendPrepare(seq, v, num)
		if ok {
			if px.sendAccept(seq, va, num) {
				px.sendDecided(seq, va, num)
				break
			}
		}
		status, _ := px.Status(seq)
		if status == Decided {
			break
		}
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	if seq < px.Min() {
		return
	}

	go func() {
		px.proposer(seq, v)
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq > px.maxdone[px.me] {
		px.maxdone[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	maxSeq := 0
	for i := range px.instances {
		if i > maxSeq {
			maxSeq = i
		}
	}
	return maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	minSeq := px.maxdone[px.me]
	for i := range px.maxdone {
		if minSeq > px.maxdone[i] {
			minSeq = px.maxdone[i]
		}
	}

	px.forgot(minSeq)
	return minSeq + 1
}

func (px *Paxos) forgot(minSeq int) {
	for i, instance := range px.instances {
		if i > minSeq {
			continue
		}
		if instance.status != Decided {
			continue
		}
		delete(px.instances, i)
	}
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq < px.Min() {
		return Forgotten, nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	if _, ok := px.instances[seq]; !ok {
		return Pending, nil
	}

	return px.instances[seq].status, px.instances[seq].va
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*InstanceInfo)
	px.maxdone = make([]int, len(px.peers))
	for i := range px.maxdone {
		px.maxdone[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
