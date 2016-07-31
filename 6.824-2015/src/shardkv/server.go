package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	Op        string
	Key       string
	Value     string
	Seq       int64
	Client    string
	Id        int64
	Config    shardmaster.Config
	Reconfig  GetShardReply
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	content    map[string] string
	seen       map[string] int64
	replies    map[string] string

	currentSeq int
	config     shardmaster.Config
}


func (kv *ShardKV) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided {
			return v.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (kv *ShardKV) checkOp(op Op) (string, Err) {
	switch op.Op {
	case "Reconfigure":
		if kv.config.Num >= op.Config.Num {
			return "", OK
		}
	case "Get", "Put", "Append":
		shard := key2shard(op.Key)
		if kv.gid != kv.config.Shards[shard] {
			return "", ErrWrongGroup
		}

		seq, exists := kv.seen[op.Client]
		if exists && op.Seq <= seq {
			return kv.replies[op.Client], OK
		}
	}
	return "", ""
}

func (kv *ShardKV) doGet(op Op) {
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.Seq
}

func (kv *ShardKV) doPut(op Op) {
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.Seq

	kv.content[op.Key] = op.Value
}

func (kv *ShardKV) doAppend(op Op) {
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.Seq

	kv.content[op.Key] = kv.content[op.Key] + op.Value
}

func (kv *ShardKV) doReconfigure(op Op) {
	info := &op.Reconfig
	for key := range info.Content {
		kv.content[key] = info.Content[key]
	}
	for client := range info.Seen {
		seq, exists := kv.seen[client]
		if !exists || seq < info.Seen[client] {
			kv.seen[client] = info.Seen[client]
			kv.replies[client] = info.Replies[client]
		}
	}
	kv.config = op.Config
}

func (kv *ShardKV) doOperation(op Op) {
	switch op.Op {
	case "Get":
		kv.doGet(op)
	case "Put":
		kv.doPut(op)
	case "Append":
		kv.doAppend(op)
	case "Reconfigure":
		kv.doReconfigure(op)
	case "GetShard":

	}

	kv.px.Done(kv.currentSeq)
	kv.currentSeq++
}

func (kv *ShardKV) runPaxos(op Op) (string, Err) {
	var ok = false
	op.Id = nrand()
	var accept Op
	for !ok {
		res, ret := kv.checkOp(op)
		if ret != "" {
			return res, ret
		}

		status, v := kv.px.Status(kv.currentSeq)
		if status == paxos.Decided {
			accept = v.(Op)
		} else {
			kv.px.Start(kv.currentSeq, op)
			accept = kv.wait(kv.currentSeq)
		}
		if accept.Id == op.Id {
			ok = true
		}
		kv.doOperation(accept)
	}
	return kv.replies[op.Client], OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, seq, ck := args.Key, args.Seq, args.Me
	reply.Value, reply.Err = kv.runPaxos(Op{Op: "Get", Key: key, Seq: seq, Client: ck})
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, value, seq, ck, op := args.Key, args.Value, args.Seq, args.Me, args.Op
	_, reply.Err = kv.runPaxos(Op{Op: op, Key: key, Value: value, Seq: seq, Client:ck })
	return nil
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	if kv.config.Num < args.Config.Num {
		reply.Err = ErrNotReady
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := args.Shard
	kv.runPaxos(Op{Op:"GetShard"})

	reply.Content = map[string]string{}
	reply.Seen = map[string]int64{}
	reply.Replies = map[string]string{}

	for key := range kv.content {
		if key2shard(key) == shard {
			reply.Content[key] = kv.content[key]
		}
	}

	for client := range kv.seen {
		reply.Seen[client] = kv.seen[client]
		reply.Replies[client] = kv.replies[client]
	}

	return nil
}

func (reply *GetShardReply) Merge(other GetShardReply) {
	for key := range other.Content {
		reply.Content[key] = other.Content[key]
	}
	for client := range other.Seen {
		seq, exists := reply.Seen[client]
		if !exists || seq < other.Seen[client] {
			reply.Seen[client] = other.Seen[client]
			reply.Replies[client] = other.Replies[client]
		}
	}
}

func (kv *ShardKV) Reconfigure(newConfig shardmaster.Config) bool {

	reConfig := GetShardReply{OK, map[string]string{}, map[string]int64{},
		map[string]string{}}

	oldConfig := &kv.config

	for i := 0 ; i < shardmaster.NShards ; i++ {
		gid := oldConfig.Shards[i]
		if newConfig.Shards[i] == kv.gid && gid != kv.gid {
			args := &GetShardArgs{i, *oldConfig}
			var reply GetShardReply
			for _, server := range oldConfig.Groups[gid] {
				ok := call(server,"ShardKV.GetShard", args, &reply)
				if ok && reply.Err == OK {
					break;
				}
				if ok && reply.Err == ErrNotReady {
					return false
				}
			}

			reConfig.Merge(reply)
		}
	}

	op := Op{Op: "Reconfigure", Config: newConfig, Reconfig: reConfig}
	kv.runPaxos(op)
	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	newConfig := kv.sm.Query(-1)
	for i := kv.config.Num+1 ; i <= newConfig.Num ; i++ {
		config := kv.sm.Query(i)
		if !kv.Reconfigure(config) {
			return
		}
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.content = map[string] string{}
	kv.seen = map[string] int64{}
	kv.replies = map[string] string{}
	kv.currentSeq = 0
	kv.config = shardmaster.Config{Num:-1}

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)


	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
