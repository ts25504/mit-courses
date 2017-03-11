package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


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

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string
	result map[int]chan Op
	ack map[int64]int
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
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
		v, ok := kv.database[args.Key]
		if ok {
			reply.Value = v
			reply.Err = OK
			kv.ack[op.Id] = op.Seq
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

func (kv *RaftKV) checkOpCommitted(index int, op Op) bool {
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}

	select {
	case o := <-ch:
		committed := o == op
		if committed {
			DPrintf("kvraft: Commit %d %v", index, op)
		}
		return committed
	case <-time.After(time.Duration(2000 * time.Millisecond)):
		DPrintf("kvraft: Timeout %d %v", index, op)
		return false
	}
}

func (kv *RaftKV) excute(op Op) {
	if op.Op == PUT {
		kv.database[op.Key] = op.Value
	} else if op.Op == APPEND {
		v, ok := kv.database[op.Key]
		if ok {
			kv.database[op.Key] = v + op.Value
		} else {
			kv.database[op.Key] = op.Value
		}
	}
	kv.ack[op.Id] = op.Seq
}

func (kv *RaftKV) checkDuplicate(op Op) bool {
	v, ok := kv.ack[op.Id]
	if ok {
		return v >= op.Seq
	}

	return false
}

func (kv *RaftKV) startSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.database)
	e.Encode(kv.ack)
	data := w.Bytes()
	kv.rf.StartSnapShot(data, index)
}

func (kv *RaftKV) readSnapshot(data []byte) {
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

func (kv *RaftKV) apply() {
	for {
		msg:= <-kv.applyCh
		if msg.UseSnapshot {
			kv.readSnapshot(msg.Snapshot)
		} else {
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
				DPrintf("kvraft: Start snapshot")
				kv.startSnapshot(index)
			}
		}
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.result = make(map[int]chan Op)
	kv.database = make(map[string]string)
	kv.ack = make(map[int64]int)

	go kv.apply()

	return kv
}
