package pbservice

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
	Op    string
	Id    int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id  int64
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
type CopyArgs struct {
	Database map[string]string
	Clients map[int64]string
}

type CopyReply struct {
	Err Err
}
