package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "time"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs   []Config // indexed by config num
	result    map[int]chan Op
	ack       map[int64]int
	configNum int
}


type Op struct {
	// Your data here.
	Op      string
	Num     int
	GIDs    []int
	Servers map[int][]string
	Shard   int
	Id      int64
	Seq     int
}

func (sm *ShardMaster) checkOpCommitted(index int, op Op) bool {
	sm.mu.Lock()
	ch, ok := sm.result[index]
	if !ok {
		ch = make(chan Op, 1)
		sm.result[index] = ch
	}
	sm.mu.Unlock()

	select {
	case o := <-ch:
		committed := o.Seq == op.Seq
		if committed {
		}
		return committed
	case <-time.After(time.Duration(2000 * time.Millisecond)):
		return false
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var op Op
	op.Servers = args.Servers
	op.Id = args.Id
	op.Seq = args.Seq
	op.Op = JOIN

	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	if !isLeader {
		return
	}

	ok := sm.checkOpCommitted(index, op)
	if ok {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var op Op
	op.GIDs = args.GIDs
	op.Id = args.Id
	op.Seq = args.Seq
	op.Op = LEAVE

	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	if !isLeader {
		return
	}

	ok := sm.checkOpCommitted(index, op)
	if ok {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var op Op
	op.Shard = args.Shard
	op.GIDs = append(op.GIDs, args.GID)
	op.Id = args.Id
	op.Seq = args.Seq
	op.Op = MOVE

	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	if !isLeader {
		return
	}

	ok := sm.checkOpCommitted(index, op)
	if ok {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var op Op
	op.Num = args.Num
	op.Id = args.Id
	op.Seq = args.Seq
	op.Op = QUERY

	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	if !isLeader {
		return
	}

	ok := sm.checkOpCommitted(index, op)
	if ok {
		reply.WrongLeader = false
		sm.mu.Lock()
		num := op.Num
		if op.Num == -1 || op.Num > sm.configNum {
			num = sm.configNum
		}
		reply.Config = sm.configs[num]
		sm.ack[op.Id] = op.Seq
		reply.Err = OK
		sm.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) newConfig() *Config {
	old := &sm.configs[sm.configNum]
	new := Config{}
	new.Groups = map[int][]string{}
	new.Num = old.Num + 1
	new.Shards = [NShards]int{}
	for gid, servers := range old.Groups {
		new.Groups[gid] = servers
	}
	for i, gid := range old.Shards {
		new.Shards[i] = gid
	}
	sm.configNum++
	sm.configs = append(sm.configs, new)
	return &sm.configs[sm.configNum]
}

func (sm *ShardMaster) getMaxShardCountGID() int {
	config := &sm.configs[sm.configNum]

	for _, gid := range config.Shards {
		if gid == 0 {
			return 0
		}
	}

	count := map[int]int{}
	max := -1
	result := 0

	for gid := range config.Groups {
		count[gid] = 0
	}

	for _, gid := range config.Shards {
		count[gid]++
	}

	for gid, c := range count {
		_, ok := config.Groups[gid]
		if ok && c > max {
			max, result = c, gid
		}
	}

	return result
}

func (sm *ShardMaster) getMinShardCountGID() int {
	config := &sm.configs[sm.configNum]

	count := map[int]int{}
	min := 257
	result := 0

	for gid := range config.Groups {
		count[gid] = 0
	}

	for _, gid := range config.Shards {
		count[gid]++
	}

	for gid, c := range count {
		_, ok := config.Groups[gid]
		if ok && c < min {
			min, result = c, gid
		}
	}

	return result
}

func (sm *ShardMaster) getOneShardByGID(gid int) int {
	config := &sm.configs[sm.configNum]

	for shard, id := range config.Shards {
		if id == gid {
			return shard
		}
	}

	return -1
}


func (sm *ShardMaster) rebalanceJoin(gid int) {
	config := &sm.configs[sm.configNum]
	i := 0

	for {
		if i == NShards / len(config.Groups) {
			break
		}
		max := sm.getMaxShardCountGID()
		shard := sm.getOneShardByGID(max)
		config.Shards[shard] = gid
		i++
	}
}

func (sm *ShardMaster) rebalanceLeave(gid int) {
	config := &sm.configs[sm.configNum]

	for {
		min := sm.getMinShardCountGID()
		shard := sm.getOneShardByGID(gid)
		if shard == -1 {
			break
		}
		config.Shards[shard] = min
	}
}

func (sm *ShardMaster) excute(op Op) {
	config := sm.newConfig()

	switch op.Op {
	case JOIN:
		for gid, servers := range op.Servers {
			_, ok := config.Groups[gid]
			if !ok {
				config.Groups[gid] = servers
				sm.rebalanceJoin(gid)
			}
		}
	case LEAVE:
		for _, gid := range op.GIDs {
			_, ok := config.Groups[gid]
			if ok {
				delete(config.Groups, gid)
				sm.rebalanceLeave(gid)
			}
		}
	case MOVE:
		if op.GIDs != nil && len(op.GIDs) > 0 {
			config.Shards[op.Shard] = op.GIDs[0]
		}
	case QUERY:
		return
	default:
	}
	sm.ack[op.Id] = op.Seq
}

func (sm *ShardMaster) checkDuplicate(op Op) bool {
	v, ok := sm.ack[op.Id]
	if ok {
		return v >= op.Seq
	}

	return false
}

func (sm *ShardMaster) apply() {
	for {
		msg := <-sm.applyCh
		if msg.UseSnapshot {

		} else {
			sm.mu.Lock()
			op := msg.Command.(Op)
			index := msg.Index
			if !sm.checkDuplicate(op) {
				sm.excute(op)
			}
			ch, ok := sm.result[index]
			if ok {
				select {
				case <-sm.result[index]:
				default:
				}
				ch <- op
			} else {
				sm.result[index] = make(chan Op, 1)
			}

			sm.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.result = make(map[int]chan Op)
	sm.ack = make(map[int64]int)
	sm.configNum = 0

	go sm.apply()

	return sm
}
