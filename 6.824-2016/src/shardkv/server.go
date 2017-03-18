package shardkv


import "shardmaster"
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
	Op       string
	Key      string
	Value    string
	Seq      int
	Id       int64
	Config   shardmaster.Config
	Transfer GetShardReply
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
	mck          *shardmaster.Clerk
	database     map[string]string
	result       map[int]chan Op
	ack          map[int64]int
	config       shardmaster.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	op.Op = Get
	op.Key = args.Key
	op.Id = args.Id
	op.Seq = args.Seq

	ok := kv.startOp(op)
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

	ok := kv.startOp(op)
	if ok {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (kv *ShardKV) startOp(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()

	select {
	case o := <-ch:
		committed := o.Seq == op.Seq
		return committed
	case <-time.After(time.Duration(2000 * time.Millisecond)):
		return false
	}
}

func (kv *ShardKV) excute(op Op) {
	switch op.Op {
	case Put:
		kv.database[op.Key] = op.Value
	case Append:
		v, ok := kv.database[op.Key]
		if ok {
			kv.database[op.Key] = v + op.Value
		} else {
			kv.database[op.Key] = op.Value
		}
	case Get:
		return
	case GetShard:
		return
	case Reconfigure:
		info := &op.Transfer
		for key := range info.Database {
			kv.database[key] = info.Database[key]
		}
		for id := range info.Ack {
			seq, exists := kv.ack[id]
			if !exists || seq < info.Ack[id] {
				kv.ack[id] = info.Ack[id]
			}
		}
		kv.config = op.Config
		return
	default:
	}
	kv.ack[op.Id] = op.Seq
}

func (kv *ShardKV) checkOp(op Op) bool {
	switch op.Op {
	case Reconfigure:
		return kv.config.Num < op.Config.Num
	case Get, Put, Append:
		return kv.checkDuplicate(op) && kv.checkShard(op)
	default:
		return true
	}
}

func (kv *ShardKV) checkShard(op Op) bool {
	shard := key2shard(op.Key)
	if kv.gid != kv.config.Shards[shard] {
		return false;
	}

	return true
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
			if !kv.checkOp(op) {
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

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	if kv.config.Num < args.Config.Num {
		reply.Err = ErrNotReady
		return
	}

	var op Op
	op.Op = GetShard
	shard := args.Shard

	ok := kv.startOp(op)
	if ok {
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Ack = map[int64]int{}
		reply.Database = map[string]string{}

		for key := range kv.database {
			if key2shard(key) == shard {
				reply.Database[key] = kv.database[key]
			}
		}

		for id := range kv.ack {
			reply.Ack[id] = kv.ack[id]
		}
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}

func (kv *ShardKV) reconfigure(newConfig shardmaster.Config) bool {
	oldConfig := &kv.config
	transfer := GetShardReply{OK, map[string]string{}, map[int64]int{}, false}

	for i := 0; i < shardmaster.NShards; i++ {
		newGid := newConfig.Shards[i]
		oldGid := oldConfig.Shards[i]
		if newGid == kv.gid && oldGid != kv.gid {
			var args GetShardArgs
			args.Shard = i
			args.Config = *oldConfig
			var reply GetShardReply
			for _, server := range oldConfig.Groups[oldGid] {
				srv := kv.make_end(server)
				ok := srv.Call("ShardKV.GetShard", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					break
				}

				if ok && reply.WrongLeader == false && reply.Err == ErrNotReady {
					return false
				}
			}
			transfer.Merge(reply)
		}
	}

	var op Op
	op.Op = Reconfigure
	op.Config = newConfig
	op.Transfer = transfer

	ok := kv.startOp(op)
	if ok {
		return true
	} else {
		return false
	}
}

func (kv *ShardKV) tick() {
	for {
		newConfig := kv.mck.Query(-1)
		for i := kv.config.Num+1; i <= newConfig.Num; i++ {
			config := kv.mck.Query(i)
			if !kv.reconfigure(config) {
				break
			}
		}

		time.Sleep(100 * time.Millisecond)
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
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.result = make(map[int]chan Op)
	kv.database = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.config = shardmaster.Config{Num:-1}

	go kv.apply()
	go kv.tick()

	return kv
}
