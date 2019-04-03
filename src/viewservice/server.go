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
	pingFromServers map[string](time.Time)
	view            View
	ackPrimary      uint
}

// for testing
const Debug = 0

func DebugPrint(format string, a ...interface{}) {
	if Debug == 1 {
		fmt.Printf(format, a...)
	}
}

func (vs *ViewServer) printView() {
	DebugPrint("current view: viewNum: %d, Primary: %s, Backup: %s\n",
		vs.view.Viewnum, vs.view.Primary, vs.view.Backup)
}

func (vs *ViewServer) createNewView(newViewNum uint, newPrimary, newBackup string) (view *View) {
	view = new(View)
	view.Viewnum = newViewNum
	view.Primary = newPrimary
	view.Backup = newBackup
	return
}

// IsAcked checks if viewservice has received ack from Primary
// Note: the mechanism is checking whether ackPrimary==Viewnum and this
// affects the implementation of ackPrimary update
func (vs *ViewServer) IsAcked() bool {
	return vs.ackPrimary == vs.view.Viewnum
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	client, clientViewnum := args.Me, args.Viewnum

	DebugPrint("----------\nCurrent view: viewNum: %d, Primary: %s, Backup: %s\n",
		vs.view.Viewnum, vs.view.Primary, vs.view.Backup)
	DebugPrint("Ping from KV server. View: %d, from: %s\n", vs.view.Viewnum, client)

	vs.printView()

	/* old code which I do not remember at all. Redo them.
	// upon very start, set first server as primary
	if clientViewnum == 0 && vs.view.Primary == "" {
		vs.view.Primary = client
		vs.view.Viewnum++
		// newv := vs.view.Viewnum + 1
		// vs.createNewView(newv, client, "")
		vs.pingFromServers[vs.view.Primary] = time.Now()
		vs.ackPrimary = 0
	} else if vs.view.Primary == client {
		// Ping from primary
		// if primary has just restarted: promote backup to primary
		if clientViewnum == 0 {
			// newv := vs.view.Viewnum + 1
			// vs.createNewView(newv, vs.view.Backup, "")
			// vs.view.Backup = client
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = client
			vs.view.Viewnum++
		} else {
			// primary in normal case
			vs.ackPrimary = clientViewnum
			vs.pingFromServers[vs.view.Primary] = time.Now()
		}
	} else if vs.view.Backup == "" && vs.IsAcked() {
		// Ping from an idle server
		// if no bakcup and a new server shows up, make it backup
		vs.view.Backup = client
		vs.view.Viewnum++
		vs.pingFromServers[vs.view.Backup] = time.Now()
	} else if vs.view.Backup == client {
		// Ping from Backup in normal case
		vs.pingFromServers[vs.view.Backup] = time.Now()
	}
	*/

	// the following logic should better be implemented with if ... else if ...
	// upon start, set the server to primary
	if vs.view.Primary == "" && clientViewnum == 0 {
		vs.view.Primary = client
		vs.view.Viewnum++
		vs.pingFromServers[client] = time.Now()
		vs.ackPrimary = clientViewnum
	} else if vs.view.Primary == client {
		/*
			If receives Ping from current primary, 2 cases:
			1. current Primay just restarted
			2. normal case: Primay is OK
		*/
		// 1. current primary just restarted, promote Backup to Primary
		if clientViewnum == 0 {
			vs.view.Primary = vs.view.Backup
			vs.view.Viewnum++
			vs.view.Backup = client
			// we do not consider this Ping valid for ackPrimary
			vs.ackPrimary = 0
		} else {
			// here is the normal case
			vs.ackPrimary = clientViewnum
			vs.pingFromServers[client] = time.Now()
		}
	} else if vs.view.Backup == "" && vs.IsAcked() {
		// if there is no Backup and incomming client is not Primary (then it is new server)
		// note that if not IsAcked, we cannot update view
		vs.view.Backup = client
		vs.view.Viewnum++
		vs.pingFromServers[client] = time.Now()
	} else if vs.view.Backup == client {
		// If receives Ping from Backup
		vs.pingFromServers[client] = time.Now()
	}

	reply.View = vs.view

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.view

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

	duration := time.Now().Sub(vs.pingFromServers[vs.view.Primary])
	is_primary_dead := duration > PingInterval*DeadPings
	if is_primary_dead && vs.IsAcked() && vs.view.Primary != "" {
		vs.view.Primary = vs.view.Backup
		vs.view.Backup = ""
		vs.view.Viewnum++
	}
	duration = time.Now().Sub(vs.pingFromServers[vs.view.Backup])
	is_backup_dead := duration > PingInterval*DeadPings
	if is_backup_dead && vs.IsAcked() && vs.view.Backup != "" {
		vs.view.Backup = ""
		vs.view.Viewnum++
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
	vs.pingFromServers = make(map[string](time.Time))

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
