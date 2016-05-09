package kvpaxos

import "crypto/rand"
import "math/big"
import "strconv"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
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
	UUID  int64
	Me    string // Identify clerk for "at-most-once" semantics
}

type PutAppendReply struct {
	Err Err
	PreValue string
}

type GetArgs struct {
	Key  string
	// You'll have to add definitions here.
	UUID int64
	Me   string
}

type GetReply struct {
	Err   Err
	Value string
}
