package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"

const ChangeLeaderInterval = time.Millisecond * 20


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	previousId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.previousId = 0
	return ck
}

func (ck *Clerk) genMsgId() int64 {
	return int64(nrand())
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("Clert GET %s", key)
	args := GetArgs{RequestId: ck.genMsgId(), PreviousId: ck.previousId, Key: key, }

	for ; ; ck.leaderId = (ck.leaderId + 1) % len(ck.servers) {
		server := ck.servers[ck.leaderId]
		reply := GetReply{}
		ok := server.Call("KVServer.Get", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey ){
			DPrintf("!!! Clert GET %s, value is %s", key, reply.Value)
			ck.previousId = args.RequestId
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("Clert PutAppend %s, %v", key, value)
	args := PutAppendArgs{RequestId: ck.genMsgId(), PreviousId: ck.previousId, Key: key, Value: value, Op: op}

	for ; ; ck.leaderId = (ck.leaderId + 1) % len(ck.servers) {
		server := ck.servers[ck.leaderId]
		reply := GetReply{}
		ok := server.Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			DPrintf("!!! Clert PutAppend %s, %v DONE", key, value)
			ck.previousId = args.RequestId
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
