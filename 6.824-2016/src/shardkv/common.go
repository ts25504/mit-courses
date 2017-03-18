package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"

	ErrNotReady = "ErrNotReady"
	Get         = "Get"
	Put         = "Put"
	Append      = "Append"
	GetShard    = "GetShard"
	Reconfigure = "Reconfigure"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id    int64
	Seq   int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id    int64
	Seq   int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type GetShardArgs struct {
	Shard  int
	Config shardmaster.Config
}

type GetShardReply struct {
	Err         Err
	Database    map[string]string
	Ack         map[int64]int
	WrongLeader bool
}

func (reply *GetShardReply) Merge(other GetShardReply) {
	for key := range other.Database {
		reply.Database[key] = other.Database[key]
	}
	for id := range other.Ack {
		seq, exists := reply.Ack[id]
		if !exists || seq < other.Ack[id] {
			reply.Ack[id] = other.Ack[id]
		}
	}
}
