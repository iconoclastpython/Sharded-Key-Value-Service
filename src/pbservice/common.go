package pbservice

import(
	"hash/fnv"
	"crypto/rand"
	"math/big"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Operation string // For PutHash
	Forward bool // Identify whether it's primary->backup forward request
	UUID string // Used for outstanding RPC request
	Me string // Clerk's identification

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	PreviousValue string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID int64
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
type ForwardArgs struct {
	Content map[string]string
}

type ForwardReply struct {
	Err Err
}

func hash(s string) unit32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func nrand() int64 {
	max := big.NewInt(int64(int64(1) << 62))
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}