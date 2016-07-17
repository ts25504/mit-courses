package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "math/big"
import (
	crand "crypto/rand"
)
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	currentSeq int
	configNum int
}


type Op struct {
	// Your data here.
	Op string
	Num int
	GID int64
	Servers []string
	Shard int
	Id int64
}

func (sm *ShardMaster) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, v := sm.px.Status(seq)
		if status == paxos.Decided {
			return v.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func GetShardByGID(gid int64, config *Config) int {
	for s, g := range config.Shards {
		if g == gid {
			return s
		}
	}
	return -1
}

func GetMaxCountGid(config *Config) int64 {
	max_count := -1
	max_gid := int64(0)
	count := map[int64]int{}
	for i := range config.Groups {
		count[i] = 0
	}
	for _, gid := range config.Shards {
		count[gid]++
	}
	for gid := range count {
		_, ok := config.Groups[gid]
		if ok && max_count < count[gid] {
			max_gid, max_count = gid, count[gid]
		}
	}
	for _, gid := range config.Shards {
		if gid == 0 {
			max_gid = 0
		}
	}

	return max_gid
}

func GetMinCountGid(config *Config) int64 {
	min_count := 1 << 32
	min_gid := int64(0)
	count := map[int64]int{}
	for i := range config.Groups {
		count[i] = 0
	}
	for _, gid := range config.Shards {
		count[gid]++
	}
	for gid := range count {
		_, ok := config.Groups[gid]
		if ok && min_count > count[gid] {
			min_gid, min_count = gid, count[gid]
		}
	}
	return min_gid
}

func (sm *ShardMaster) balanceJoin(gid int64) {
	config := &sm.configs[sm.configNum]
	index := 0
	for {
		if index == NShards / len(config.Groups) {
			break
		}
		max_gid := GetMaxCountGid(config)
		shard := GetShardByGID(max_gid, config)
		config.Shards[shard] = gid
		index++
	}
}

func (sm *ShardMaster) balanceLeave(gid int64) {
	config := &sm.configs[sm.configNum]
	for {
		min_gid := GetMinCountGid(config)
		shard := GetShardByGID(gid, config)
		if shard == -1 {
			break
		}
		config.Shards[shard] = min_gid
	}
}

func (sm *ShardMaster) newConfig() *Config {
	old := &sm.configs[sm.configNum]
	new := Config{}
	new.Groups = map[int64][]string{}
	new.Num = old.Num + 1
	new.Shards = [NShards]int64{}
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

func (sm *ShardMaster) doJoin(gid int64, servers []string) {
	config := sm.newConfig()
	_, ok := config.Groups[gid]
	if !ok {
		config.Groups[gid] = servers
		sm.balanceJoin(gid)
	}
}

func (sm *ShardMaster) doLeave(gid int64) {
	config := sm.newConfig()
	_, ok := config.Groups[gid]
	if ok {
		delete(config.Groups, gid)
		sm.balanceLeave(gid)
	}
}

func (sm *ShardMaster) doMove(gid int64, shard int) {
	config := sm.newConfig()
	config.Shards[shard] = gid
}

func (sm *ShardMaster) doQuery(num int) Config {
	if num == -1 {
		return sm.configs[sm.configNum]
	} else {
		return sm.configs[num]
	}
}

func (sm *ShardMaster) doOperation(op Op) Config {
	config := Config{}
	switch op.Op {
	case "Join":
		sm.doJoin(op.GID, op.Servers)
	case "Leave":
		sm.doLeave(op.GID)
	case "Move":
		sm.doMove(op.GID, op.Shard)
	case "Query":
		config = sm.doQuery(op.Num)
	default:
	}
	sm.px.Done(sm.currentSeq)
	sm.currentSeq += 1
	return config
}

func (sm *ShardMaster) runPaxos(op Op) Config {
	op.Id = nrand()
	var accept Op
	for {
		status, v := sm.px.Status(sm.currentSeq)
		if status == paxos.Decided {
			accept = v.(Op)
		} else {
			sm.px.Start(sm.currentSeq, op)
			accept = sm.wait(sm.currentSeq)
		}
		config := sm.doOperation(accept)
		if accept.Id == op.Id {
			return config
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Op: "Join", GID: args.GID, Servers: args.Servers}
	sm.runPaxos(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Op: "Leave", GID: args.GID}
	sm.runPaxos(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Op: "Move", GID: args.GID, Shard: args.Shard}
	sm.runPaxos(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Op: "Query", Num: args.Num}
	reply.Config = sm.runPaxos(op)
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.currentSeq = 0
	sm.configNum = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
