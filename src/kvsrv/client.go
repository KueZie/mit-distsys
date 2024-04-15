package kvsrv

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	name	  string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.name = fmt.Sprintf("Clerk-%v", nrand())

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, Clerk: ck.name}
	reply := GetReply{}

	retry := 0
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			break
		}
		ClientDebug(args.Clerk, "Get(%v) failed, no server response, retrying", key)
		retry++
		time.Sleep(time.Duration(retry * 10) * time.Millisecond)
		if retry > 10 {
			log.Fatalf("Get(%v) failed, no server response after 10 retries", key)
		}
	}


	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{Key: key, Value: value, Clerk: ck.name, MsgId: nrand()}
	reply := PutAppendReply{}

	if op != "Put" && op != "Append" {
		log.Fatalf("PutAppend: invalid op %v\nvalid ops \"Put\" or \"Append\"", op)
	}

	rpcCall := fmt.Sprintf("KVServer.%s", op) // "KVServer.Put" or "KVServer.Append"

	retry := 0
	for {
		ok := ck.server.Call(rpcCall, &args, &reply)
		if ok {
			break
		}
		ClientDebug(args.Clerk, "%v(%v) failed, no server response, retrying", op, key)
		retry++
		time.Sleep(time.Duration(retry * 10) * time.Millisecond)
		if retry > 10 {
			log.Panicf("%v(%v) failed, no server response after 100 retries", op, key)
		}
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}