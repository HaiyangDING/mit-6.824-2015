package mapreduce

import (
	"container/list"
	"sync"
)
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.

	// add any additional item here
	state int // 0 means ready
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	fmt.Print("Start RunMaster() here...")
	
	// register workers from chanel
	go func() {
		for {
			workerAdd := <- mr.registerChannel
			mr.mu.Lock()
			mr.Workers[workerAdd] = &WorkerInfo{workerAdd, 0}
			mr.mu.Unlock()
			fmt.Print("Worker %v registered.\n", workerAdd)
		}
	}
	
	// handle job to each worker and wait until complete
	var wg sync.WaitGroup
	

	return mr.KillWorkers()
}