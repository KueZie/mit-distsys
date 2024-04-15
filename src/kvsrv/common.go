package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Clerk string
	MsgId int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Clerk string
	Key string
}

type GetReply struct {
	Value string
}
