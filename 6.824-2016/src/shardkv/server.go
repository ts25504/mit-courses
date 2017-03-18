package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "time"
import "bytes"


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string
	Seq   int
	Id    int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string
	result map[int]chan Op
	ack map[int64]int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	op.Op = GET
	op.Key = args.Key
	op.Id = args.Id
	op.Seq = args.Seq

	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	if !isLeader {
		return
	}

	ok := kv.checkOpCommitted(index, op)
	if ok {
		reply.WrongLeader = false
		kv.mu.Lock()
		v, ok := kv.database[args.Key]
		if ok {
			reply.Value = v
			reply.Err = OK
			kv.ack[op.Id] = op.Seq
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op
	op.Op = args.Op
	op.Key = args.Key
	op.Value = args.Value
	op.Id = args.Id
	op.Seq = args.Seq

	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	if !isLeader {
		return
	}

	ok := kv.checkOpCommitted(index, op)
	if ok {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (kv *ShardKV) checkOpCommitted(index int, op Op) bool {
	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()

	select {
	case o := <-ch:
		committed := o == op
		return committed
	case <-time.After(time.Duration(2000 * time.Millisecond)):
		return false
	}
}

func (kv *ShardKV) excute(op Op) {
	switch op.Op {
	case PUT:
		kv.database[op.Key] = op.Value
	case APPEND:
		v, ok := kv.database[op.Key]
		if ok {
			kv.database[op.Key] = v + op.Value
		} else {
			kv.database[op.Key] = op.Value
		}
	case GET:
		return
	default:
	}
	kv.ack[op.Id] = op.Seq
}

func (kv *ShardKV) checkDuplicate(op Op) bool {
	v, ok := kv.ack[op.Id]
	if ok {
		return v >= op.Seq
	}

	return false
}

func (kv *ShardKV) startSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.ack)
	data := w.Bytes()
	go kv.rf.StartSnapshot(data, index)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var lastIncludeIndex int
	var lastIncludeTerm int

	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)

	d.Decode(&kv.database)
	d.Decode(&kv.ack)
}

func (kv *ShardKV) apply() {
	for {
		msg := <-kv.applyCh
		if msg.UseSnapshot {
			kv.mu.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		} else {
			kv.mu.Lock()
			op := msg.Command.(Op)
			index := msg.Index
			if !kv.checkDuplicate(op) {
				kv.excute(op)
			}
			ch, ok := kv.result[index]
			if ok {
				select {
				case <-kv.result[index]:
				default:
				}
				ch <- op
			} else {
				kv.result[index] = make(chan Op, 1)
			}

			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				kv.startSnapshot(index)
			}
			kv.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.result = make(map[int]chan Op)
	kv.database = make(map[string]string)
	kv.ack = make(map[int64]int)

	go kv.apply()

	return kv
}
