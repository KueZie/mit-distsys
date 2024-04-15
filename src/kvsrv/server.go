package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = false

func DPrintf(name string, clerk string, format string, a ...interface{}) (n int, err error) {
	if Debug {
		new := fmt.Sprintf("[%v](%v) %v", name, clerk, format)
		log.Printf(new, a...)
	}
	return
}

func ClientDebug(msg string, format string, a ...interface{}) {
	DPrintf("Client", msg, format, a...)
}

func ServerDebug(msg string, format string, a ...interface{}) {
	DPrintf("Server", msg, format, a...)
}

type CacheValue struct {
	MsgId  int64  // Last message id received from the client
	Value  string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]string

	cache map[string]CacheValue
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value := kv.store[args.Key]

	// If the key does not exist, return an empty string
	if value == "" {
		ServerDebug(args.Clerk, "Get(%v) -> empty", args.Key)
	} else {
		ServerDebug(args.Clerk, "Get(%v) -> %v", args.Key, value)
	}

	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// If the client has already sent this request, ignore it
	if kv.cache[args.Clerk].MsgId == args.MsgId {
		reply.Value = ""
		return
	}

	if args.Value == "" {
		ServerDebug(args.Clerk, "Put(%v, empty)", args.Key)
	} else {
		ServerDebug(args.Clerk, "Put(%v, %v)", args.Key, args.Value)
	}

	kv.store[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// If the client has already sent this request, ignore it
	if cached, ok := kv.cache[args.Clerk]; ok {
		if cached.MsgId == args.MsgId {
			reply.Value = cached.Value
			return
		}
	}

	old := kv.store[args.Key]

	if args.Value == "" {
		ServerDebug(args.Clerk, "Append(%v, empty) -> %v", args.Key, old)
	} else {
		ServerDebug(args.Clerk, "Append(%v, %v) -> %v", args.Key, args.Value, old)
	}

	reply.Value = old // Return the old value

	// Store the msgId and length of the value in the cache, so if we receive a duplicate request
	// we can return the value that would have been returned if the request was not a duplicate
	// aka the value of the store at the time of the request
	kv.cache[args.Clerk] = CacheValue{MsgId: args.MsgId, Value: old}

	kv.store[args.Key] = old + args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// Init store variables
	kv.store = make(map[string]string)
	// Holds the most recent request from each client
	kv.cache = make(map[string]CacheValue)


	return kv
}
