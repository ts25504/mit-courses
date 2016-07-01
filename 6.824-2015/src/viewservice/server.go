package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	currentview View
	recenttime map[string]time.Time
	ack bool
}

func (vs *ViewServer) promote() {
	vs.currentview.Primary = vs.currentview.Backup
	vs.currentview.Backup = ""
	vs.currentview.Viewnum += 1
	vs.ack = false
}

func (vs *ViewServer) removeBackup() {
	vs.currentview.Backup = ""
	vs.ack = false
}

func (vs *ViewServer) acceptPrimary(primary string) {
	vs.currentview.Primary = primary
	vs.currentview.Viewnum += 1
	vs.ack = false
}

func (vs *ViewServer) acceptBackup(backup string) {
	vs.currentview.Backup = backup
	vs.currentview.Viewnum += 1
	vs.ack = false
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	switch args.Me {
		case vs.currentview.Primary:
			if args.Viewnum == vs.currentview.Viewnum {
				vs.ack = true
				vs.recenttime[vs.currentview.Primary] = time.Now()
			} else {
				if vs.ack {
					vs.promote()
				}
			}
		case vs.currentview.Backup:
			if args.Viewnum == vs.currentview.Viewnum {
				vs.recenttime[vs.currentview.Backup] = time.Now()
			} else {
				if vs.ack {
					vs.removeBackup()
				}
			}
		default:
			if vs.currentview.Primary == "" {
				vs.acceptPrimary(args.Me)
			} else if vs.currentview.Backup == "" && vs.ack {
				vs.acceptBackup(args.Me)
			}
	}

	reply.View = vs.currentview

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.currentview

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.ack == false {
		return
	}

	t1 := time.Now()
	if vs.currentview.Primary != "" {
		t2 := vs.recenttime[vs.currentview.Primary]

		if t1.Sub(t2) > DeadPings * PingInterval {
			vs.promote()
		}
	}

	if vs.currentview.Backup != "" {
		t2 := vs.recenttime[vs.currentview.Backup]

		if t1.Sub(t2) > DeadPings * PingInterval {
			vs.removeBackup()
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.recenttime = make(map[string]time.Time)
	vs.ack = false
	vs.currentview = View{}

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
