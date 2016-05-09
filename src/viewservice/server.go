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
	// 1. map viewservice name to current time
	// 2. keep track of the current view
	// 3. keep track of whether the primary is ACKed (PingArgs.Viewnum)
	lastSeen map[string]time.Time
	curView View
	isPrimaryAcked bool
}

func (vs *ViewServer) changeView(n unit, p, b string){
	vs.curView = View{n, p, b}
	vs.isPrimaryAcked = false
}

func (vs *ViewServer) promoteBackup() bool{
	if !vs.isPrimaryAcked {
		return false
	}

	if vs.curView.Backup == "" {
		return false
	}

	vs.changeView(vs.curView.Viewnum+1, vs.curView.Backup, "")

	return true
}

func (vs *ViewServer) removeBackup() bool{
	if !vs.isPrimaryAcked {
		return false
	}

	vs.changeView(vs.curView.Viewnum+1, vs.curView.Primary, "")

	return true
}

func (vs *ViewServer) addServer(id string) bool{
	if !vs.isPrimaryAcked {
		return false
	}

	if vs.curView.Primary == "" {
		vs.changeView(1, id, "")
		return true
	}

	if vs.curView.Backup == "" {
		vs.changeView(vs.curView.Viewnum+1, vs.curView.Primary, id)
		return true
	}

	return false
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	id, viewnum := args.Me, args.Viewnum

	switch id {
	case vs.curView.Primary:
		if viewnum == vs.curView.Viewnum {
			vs.isPrimaryAcked = true
			vs.lastSeen[vs.curView.Primary] = time.Now()
		} else {
			vs.promoteBackup()
		}
	case vs.curView.Backup:
		if viewnum == vs.curView.Viewnum {
			vs.lastSeen[vs.curView.Backup] = time.Now()
		} else {
			vs.removeBackup()
		}
	default:
		vs.addServer(id)
	}

	reply.View = vs.curView

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.curView

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

	for id, t := range vs.lastSeen {
		if time.Since(t) >= DeadPings*PingInterval {
			switch id {
			case vs.curView.Primary:
				vs.promoteBackup()
			case vs.curView.Backup:
				vs.removeBackup()
			}
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
