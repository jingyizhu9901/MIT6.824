package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	RequestId int64
	PreviousId int64
	Key   string
	Value string
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	RequestId int64
	PreviousId int64
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
