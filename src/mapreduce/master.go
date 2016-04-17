package mapreduce

import (
	"container/list"
)
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
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

	// Defining the channel
	idleWorkerChan := make(chan string)
	jobChan := make(chan *DoJobArgs)
	jobDoneChan := make(chan int)

	//find an idle worker: by registration or when job finishes
	getNextWorker := func() string {
		var address string

		select {
		case address = <-mr.registerChannel:
			mr.Workers[address] = &WorkerInfo{address}
		case address = <-idleWorkerChan:
		}

		return address
	}

	doJob := func(worker string, job *DoJobArgs) {
		var reply DoJobReply

		jobDone := call(worker, "Worker.DoJob", job, &reply)

		if jobDone {
			jobDoneChan <- 1
			idleWorkerChan <- worker
		} else {
			fmt.Printf("RunMaster RPC call error with worker: %s ... Redistributing the job", worker)
			jobChan <- job
		}
	}

	// start job distributing, for either Map or Reduce
	// remember to close the jobChan when Reduce's are all done
	go func() {
		for job := range jobChan {
			worker := getNextWorker()
			go func(job *DoJobArgs) {
				doJob(worker, job)
			}(job)
		}
	}()

	// start adding Map job to jobChan
	go func() {
		for i := 0; i < mr.nMap; i++ {
			job := &DoJobArgs{mr.file, Map, i, mr.nMap}
			jobChan <- job
		}
	}()

	// wait for the number of Map's are done
	for i := 0; i < mr.nMap; i++ {
		<-jobDoneChan
	}

	// start adding Reduce job to jobChan
	go func() {
		for i := 0; i < mr.nReduce; i++ {
			job := &DoJobArgs{mr.file, Reduce, i, mr.nReduce}
			jobChan <- job
		}
	}()

	// wait for the number of Reduce's are done
	for i := 0; i < mr.nReduce; i++ {
		<-jobDoneChan
	}

	// close jobChan so that the jobChan loop could finish
	close(jobChan)

	return mr.KillWorkers()
}
