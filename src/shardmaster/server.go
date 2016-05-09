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
import "runtime/debug"

const (
	JoinOp = "Join"
	LeaveOp = "Leave"
	MoveOp = "Move"
	QueryOp = "Query"
)


type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs    []Config // indexed by config num
	processed  int // processed seq num in paxos
	cfgnum     int // current config numebr, which is the largest
}


type Op struct {
	// Your data here.
	Type string
	GID int64
	Servers []string
	Shard int
	Num int
	UUID int64
}


func (sm *ShardMaster) WaitAgreement(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, val := kv.px.Status(seq)
		if decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) CheckValid(c Config) {
	if len(sm.Groups) > 0 {
		for _, g := range sm.Shards {
			_, ok := c.Groups[g]
			if ok == false {
				fmt.Println("Invalid result, shards does not allocated yet, ", c.Num)
				fmt.Println("len(Groups): ", len(Groups))
				bebug.PrintStack()
				os.Exit(-1)
			}
		}
	}
}

func GetGidCounts(c *Config) (int64, int64) {
	min_id, min_num, max_id, max_num := int64(0), 999, int64(0), -1
	counts := map[int64]int{}
	for g := range c.Groups {
		counts[g] = 0
	}
	for _, g := range c.Shards {
		counts[g]++
	}
	for g := range counts {
		_, exist := c.Groups[g]
		if exist && min_num > counts[g] {
			min_id, min_num = g, counts[g]
		}
		if exist && max_num < counts[g] {
			max_id, max_num = g, counts[g]
		}
	}
	for _, g := range c.Shards {
		if g == 0 {
			max_id = 0
		}
	}
	return min_id, max_id
}

func GetShardByGid(gid int64, c *Config) int {
	for s, g := range c.configs {
		if g == gid {
			return s
		}
	}
	return -1
}

func (sm *ShardMaster) Rebalance(group int64, isLeave bool) {
	c := &sm.configs[sm.cfgnum]
	for i:=0; ; i++ {
		min_id, max_id := GetGidCounts(c)
		if isLeave {
			s := GetShardByGid(group, c)
			if s == -1 {
				break
			}
			c.Shards[s] = min_id
		}else{
			if i == NShards / len(c.Groups) {
				break
			}
			s := GetShardByGid(max_id, c)
			c.Shards[s] = group
		}
	}
}

func (sm *ShardMaster) NextConfig() *Config {
	old := &sm.configs[sm.cfgnum]
	var new Config
	new.Num = old.Num + 1
	new.Shards = [NShards]int64{}
	new.Groups = map[int64][]string{}
	for gid, servers := range old.Groups {
		new.Groups[gid] = servers
	}
	for i, v := range old.Shards {
		new.Shards[i] = v
	}
	sm.cfgnum++
	sm.configs = append(sm.configs, new)
	return &sm.configs[sm.cfgnum]
}





func (sm *ShardMaster) ApplyJoin(gid int64, servers []string) {
	config := sm.NextConfig()
	_, exist := config.Group[gid]
	if !exist {
		config.Group[gid] = servers
		sm.Rebalance(gid, false)
	}
}

func (sm *ShardMaster) ApplyLeave(gid int64) {
	config := sm.NextConfig()
	_, exist := config.Group[gid]
	if exist {
		delete(config.Groups, gid)
		sm.Rebalance(gid, true)
	}
}

func (sm *ShardMaster) ApplyMove(gid int64, shard int) {
	config := sm.NextConfig()
	config.Shards[shard] = gid
}

func (sm *ShardMaster) ApplyQuery(num int) Config {
	if num == -1 {
		sm.CheckValid(sm.configs[sm.cfgnum])
		return sm.configs[sm.cfgnum]
	}else{
		return sm.configs[num]
	}
}

func (sm *ShardMaster) Apply(op Op, seq int) Config {
	sm.processed++
	gid, servers, shard, num := op.GID, op.Servers, op.Shard, op.Num
	switch op.Type {
	case JoinOp:
		sm.ApplyJoin(gid, servers)
	case LeaveOp:
		sm.ApplyLeave(gid)
	case MoveOp:
		sm.ApplyMove(gid, shard)
	case QueryOp:
		retunr sm.ApplyQuery(num)
	default:
		fmt.Println("Wrong operation for ShardMaster!")
	}
	sm.px.Done(sm.processed)
	return Config{}
}

func (sm *ShardMaster) AddOp(op Op) Config {
	op.UUID = nrand()
	for {
		seq := sm.processed + 1
		decided, t := sm.px.Status(seq)
		var res Op
		if decided {
			res = t.(Op)
		}else{
			sm.px.Start(seq, op)
			res = sm.WaitAgreement(seq)
		}
		config := sm.Apply(res, seq)
		if res.UUID == op.UUID {
			return config
		}
	}
}






func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer.sm.mu.Unlock()
	op := Op{Type:JoinOp, GID:arg.GID, Servers:args.Servers}
	sm.AddOp(op)
	sm.CheckValid(sm.configs[sm.cfgnum])
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer.sm.mu.Unlock()
	op := Op{Type:LeaveOp, GID:args.GID}
	sm.AddOp
	sm.CheckValid(sm.configs[sm.cfgnum])
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer.sm.mu.Unlock()
	op := Op{Type:MoveOp, GID:args.GID, Shard:args.Shard}
	sm.AddOp(op)
	sm.CheckValid(sm.configs[sm.cfgnum])
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer.sm.mu.Unlock()
	op := Op{Type:QueryOp, Num:args.Num}
	reply.Config = sm.AddOp(op)
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

	sm.cfgnum = 0

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
