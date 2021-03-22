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
	args := GetArgs{RequestId: ck.genMsgId(), Key: key, }
	leaderId := ck.leaderId
	for {
		reply := GetReply{}
		if ck.servers[leaderId].Call("KVServer.Get", &args, &reply) {
			switch reply.Err {
			case OK:
				DPrintf("GET key = %s, value = %s", key, reply.Value)
				ck.leaderId = leaderId
				return reply.Value
			case ErrNoKey:
				DPrintf("GET err no key, ket = %s", key)
				ck.leaderId = leaderId
				return ""
			case ErrWrongLeader:
				time.Sleep(ChangeLeaderInterval)
				leaderId = (leaderId + 1) % len(ck.servers)
				continue
			}
		} else {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
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
	args := PutAppendArgs{RequestId: ck.genMsgId(), Key: key, Value: value, Op: op}
	leaderId := ck.leaderId
	for {
		reply := GetReply{}
		if ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply) {
			switch reply.Err {
			case OK:
				DPrintf("PUT key = %v, value = %v ", key, value)
				ck.leaderId = leaderId
				return
			case ErrNoKey:
				DPrintf("PUT err no key, key = %s", key)
				ck.leaderId = leaderId
			case ErrWrongLeader:
				time.Sleep(ChangeLeaderInterval)
				leaderId = (leaderId + 1) % len(ck.servers)
				continue
			}
		} else {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
