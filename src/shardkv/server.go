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


const (
	Debug = 0
	GetOp = 1
	PutOp = 2
	AppendOp = 3
	ReconfigOp = 4
	GetShardOp = 5
)
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	Tyep      int
	Key       string
	Value     string
	Decided   bool
	SeqNum    int64 // seq_num from certain client
	UUID      int64 // unique identify the Op instance
	Client    string
	Config    shardmaster.Config // processed new configration for Reconfig
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
	content    map[string]string // key/value content
	seen       map[string]int64  // seen outstanding client request, maintaining at-most-once semantics
	replies    map[string]string // map (client_name) => last outstanding reply result
	processed  int               // up to this seq_num, all has been applied in content dict{}
	config     shardmaster.config
}



func PrintOp(op Op) string {
	switch op.Type {
	case PutOp:
		return fmt.Sprintf("put key:%s value:%s decided:%t uuid:%d", op.Key, op.Value, op.Decided, op.UUID)
	case AppendOp:
		return fmt.Sprintf("append key:%s value:%s decided:%t uuid:%d", op.Key, op.Value, op.Decided, op.UUID)
	case GetOp:
		return fmt.Sprintf("get key:%s uuid:%d", op.Key, op.UUID)
	case GetShardOp:
		return fmt.Sprintf("gshard uuid:%d", op.UUID)
	case ReconfigOp:
		return fmt.Sprintf("recfg uuid:%d", op.UUID)
	}
}



func (kv *ShardKV) WaitAgreement(seq int) Op {
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



func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	if kv.config.Num < args.Config.Num {
		reply.Err = ErrNotReady
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := args.Shard
	kv.AppOp(Op{Type:GetShardOp})

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



func (kv *ShardKV) ApplyGet(op Op) {
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.SeqNum
}



func (kv *ShardKV) ApplyPut(op Op) {
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.SeqNum
	if op.Decided {
		kv.content[op.Key] = NextValue(previous, op.Value)
	}else{
		kv.content[op.Key] = op.Value
	}
}



func (kv *ShardKV) ApplyAppend(op Op) {
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.SeqNum
	if op.Decided {
		kv.content[op.Key] += op.Value
	}else{
		kv.ApplyPut(op)
	}
}



func (kv *ShardKV) ApplyReconfigure(op Op) {
	info := &op.Reconfig
	for key := range info.Content {
		kv.content[key] = info.Content[key]
	}
	for client := range info.Seen {
		seqnum, exists := kv.seen[client]
		if !exists || seqnum < info.Seen[client] {
			kv.seen[client] = info.Seen[client]
			kv.replies[client] = info.Replies[client]
		}
	}
	kv.config = op.Config
}



func (kv *ShardKV) Apply(op Op) {
	switch op.Type {
	case GetOp:
		kv.ApplyGet(op)
	case PutOp:
		kv.ApplyPut(op)
	case AppendOp:
		kv.ApplyAppend(op)
	case ReconfigOp:
		kv.ApplyReconfigure(op)
	case GetShardOp:
		// do nothing
	}
	kv.processed++
	kv.px.Done(kv.processed)
}



func (kv *ShardKV) CheckOp(op Op) (Err, string) {
	switch op.Type {
	case ReconfigOp:
		// check current config
		if kv.config.Num >= op.config.Num {
			return OK, ""
		}
	case PutOp, AppendOp, GetOp:
		// check shard responsibility
		shard := key2shard(op.Key)
		if kv.gid != kv.config.Shards[shard] {
			return ErrWrongGroup, ""
		}
		// check seen
		seqnum, exists := kv.seen[op.Client]
		if exists && op.SeqNum <= seqnum {
			return OK, kv.replies[op.Client]
		}
	}
	return "", ""
}



func (kv *ShardKV) AddOp(op Op) (Err, string) {
	var ok = false
	op.UUID = nrand()
	for !ok {
		result, ret := kv.CheckOp(op)
		if result != "" {
			return result, ret
		}
		seq := kv.processed + 1
		decided, t := kv.px.Status(seq)
		var res Op
		if decided {
			res = t.(Op)
		}else{
			kv.px.Start(seq, op)
			res = kv.WaitAgreement(seq)
		}
		ok = res.UUID == op.UUID
		kv.Apply(res)
	}
	return OK, kv.replies[op, Client]
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, seqnum, ck := args.Key, args.SeqNum, args.Me
	reply.Err, reply.Value = kv.AddOp(Op{Type:GetOp, Key:key, SeqNum:seqnum, Client:ck})
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Op == "Put" {
		op := Op{Type:PutOp, Key:args.Key, Value:args.Value, Decided:args.Decided, SeqNum:args.SeqNum, Client:args.Me}
	}else if args.Op == "Append" {
		op := Op{Type:AppendOp, Key:args.Key, Value:args.Value, Decided:args.Decided, SeqNum:args.SeqNum, Client:args.Me}
	}
	
	reply.Err, reply.PreValue = kv.AddOp(op)
	return nil
}



func (reply *GetShardReply) Merge(other GetShardReply) {
	for key := range other.Content {
		reply.Content[key] = other.Content[key]
	}

	for client := range other.Seen {
		uuid, exists := reply.Seen[client]
		if !exists || uuid < other.Seen[client] {
			reply.Seen[client] = other.Seen[client]
			reply.Replies[client] = other.Replies[client]
		}
	}
}


// Return success or failure
func (kv *ShardKV) Reconfigure(newcfg shardmaster.Config) bool {
	// get shard
	reconfig := GetShardReply(OK, map[string]string{}, map[string]int64, map[string]string{})
	oldcfg := &kv.config
	for i := 0; i < shardmaster.NShards; i++ {
		gid := oldcfg.Shards[i]
		if newcfg.Shards[i] == kv.gid && gid != kv.gid {
			args := &GetShardArgs{i, *oldcfg}
			var reply GetShardReply
			for _, srv := range oldcfg.Group[gid] {
				ok := call(srv, "ShardKV.GetShard", args, &reply)
				if ok && reply.Err == OK {
					break
				}
				if ok && reply.Err == ErrNotReady {
					return false
				}
			}
			reconfig.Merge(reply)
		}
	}
	op := Op{Type:ReconfigOp, Config:newcfg, Reconfig:reconfig}
	kv.AddOp(op)
	return true
}



//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newcfg := kv.sm.Query(-1)
	for i := kv.config.Num + 1; i <= newcfg.Num; i++ {
		cfg := kv.sm.Query(i)
		if !kv.Reconfigure(cfg) {
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
	kv.config = shardmaster.Config{Num:-1}
	kv.content = map[string]string{}
	kv.seen = map[string]int64{}
	kv.replies = map[string]string{}
	kv.processed = 0

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
