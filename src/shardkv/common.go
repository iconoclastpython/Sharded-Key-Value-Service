package shardkv

import "shardmaster"
import "crypto/rand"
import "strconv"
import "math/big"
import "hash/fnv"

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
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqNum int64
	Me string // Identify clerk for at-most-once semantics
}

type PutAppendReply struct {
	Err Err
	PreValue string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SeqNum int64
	Me string
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	Shard int
	Config shardmaster.Config
}

type GetShardReply struct {
	Err Err
	Content map[string]string
	Seen map[string]int64
	Replies map[string]string
}

func hash(s string) unit32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func NextValue(hpre string, val string) string {
	h := hash(hpre + val)
	return strconv.Itoa(int(h))
}