package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	RequestId	int64
	PreviousId  int64
	Key			string
	Value 		string
	Op			string // "Get", "Put", "Append"
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data		  map[string]string	// key-value pair
	notifyChanMap map[int]chan notifyArgs // index returned from raft - chan to notify RPC handlers
	executedRequest map[int64]bool // a set stores requests that has been executed. It is used to prevent duplication
}

// used to notify RPC handlers
type notifyArgs struct {
	Term  int
	Value string
	Err   Err
}

func (kv *KVServer) notifyIfPresent(index int, reply notifyArgs) {
	if ch, ok := kv.notifyChanMap[index]; ok {
		ch <- reply
		DPrintf("!!! PRESENT and NOTIFY")
		delete(kv.notifyChanMap, index)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	op := Op{RequestId: args.RequestId, PreviousId: args.PreviousId, Key: args.Key, Value: "", Op: "Get"}
	index, term, ok := kv.rf.Start(op)
	if !ok {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	notifyCh := make(chan notifyArgs)
	kv.notifyChanMap[index] = notifyCh
	kv.mu.Unlock()
	result := <-notifyCh
	if result.Term != term {
		reply.Err = ErrWrongLeader
	} else {
		reply.Value = result.Value
		reply.Err = result.Err
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	// this request is duplicated
	if exist := kv.executedRequest[args.RequestId]; exist{
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	op := Op{RequestId: args.RequestId, PreviousId: args.PreviousId, Key: args.Key, Value: args.Value, Op: args.Op}
	index, term, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	notifyCh := make(chan notifyArgs)
	kv.notifyChanMap[index] = notifyCh
	kv.mu.Unlock()
	result := <-notifyCh
	if result.Term != term {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = result.Err
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) handleValidCommand(msg raft.ApplyMsg) {
	cmd := msg.Command.(Op)
	delete(kv.executedRequest, cmd.PreviousId) // free server memory quickly
	result := notifyArgs{Term: msg.CommandTerm, Value: "", Err: OK}
	if cmd.Op == "Get" {
		if v, ok := kv.data[cmd.Key]; ok {
			result.Value = v
		} else {
			result.Value = ""
			result.Err = ErrNoKey
		}
	} else {
		if exists := kv.executedRequest[cmd.RequestId]; !exists { // execute the same RequestId only once
			if cmd.Op == "Put" {
				kv.data[cmd.Key] = cmd.Value
			} else {
				if v, ok := kv.data[cmd.Key]; ok {
					kv.data[cmd.Key] = v + cmd.Value
				} else {
					kv.data[cmd.Key] = cmd.Value
				}
			}
			kv.executedRequest[cmd.RequestId] = true
		}
	}
	kv.notifyIfPresent(msg.CommandIndex, result)
}

// keep reading applyCh
func (kv *KVServer) run() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if msg.CommandValid {
				kv.handleValidCommand(msg)
			} else { // command not valid
				DPrintf("!!! Received invalid Command")
				if _, ok := msg.Command.(string); ok {
					reply := notifyArgs{Term: msg.CommandTerm, Value: "", Err: ErrWrongLeader}
					kv.notifyIfPresent(msg.CommandIndex, reply)
				}
			}
			kv.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.data = make(map[string]string)
	kv.notifyChanMap = make(map[int]chan notifyArgs)
	kv.executedRequest = make(map[int64]bool)

	go kv.run()

	return kv
}
