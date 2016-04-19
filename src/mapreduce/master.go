package mapreduce

import (
	"container/list"
)
import "fmt"

import "log"
import "sync"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	state int // -1: error; 0: idle; 1: busy
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
	/*
		// Method of [William Cheung](https://github.com/william-cheung/mit-6.824-2015/blob/master/src/mapreduce/master.go)
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
				job := &DoJobArgs{mr.file, Map, i, mr.nReduce}
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
				job := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
				jobChan <- job
			}
		}()

		// wait for the number of Reduce's are done
		for i := 0; i < mr.nReduce; i++ {
			<-jobDoneChan
		}

		// close jobChan so that the jobChan loop could finish
		close(jobChan)
	*/
	// Method of [norlanliu](https://github.com/norlanliu/mit-6.824/blob/master/src/mapreduce/master.go)

	// Register all worker from mr.registerChannel. Use goroutines!
	go func() {
		for {
			workerAdd := <-mr.registerChannel
			mr.mu.Lock()
			mr.Workers[workerAdd] = &WorkerInfo{workerAdd, 0}
			mr.mu.Unlock()
			log.Printf("Registered worker %s", workerAdd)
		}
	}()

	// Start handling Map job
	var wg sync.WaitGroup
	var worker string
	for i := 0; i < mr.nMap; i++ {
		// use loop to find an idel worker
		for {
			mr.mu.Lock()
			for _, w := range mr.Workers {
				if w.state == 0 {
					worker = w.address
					w.state = 1
					break
				}
			}
			mr.mu.Unlock()
			if worker != "" {
				// when we find a worker, break to assign job to this worker
				log.Printf("Find available worker: %s", worker)
				break
			}
		}

		// Let the work do the Map job
		log.Printf("Assigning Map to worker: %s", worker)
		job_args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
		// add one more goroutine to the wait list
		wg.Add(1)
		go func() {
			defer wg.Done()
			var job_reply DoJobReply
			// RPC call, recursively calling as long as the job is not done
			ok := call(worker, "Worker.DoJob", job_args, &job_reply)
			for ok == false && job_reply.OK == false {
				mr.Workers[worker].state = -1
				log.Printf("Worker: %s failed, try to find a new one", worker)
				// use loop to find an idel worker
				for {
					mr.mu.Lock()
					for _, w := range mr.Workers {
						if w.state == 0 {
							worker = w.address
							w.state = 1
							break
						}
					}
					mr.mu.Unlock()
					if worker != "" {
						// when we find a worker, break to assign job to this worker
						log.Printf("Find available worker: %s", worker)
						break
					}
				}
				// Let the work do the Map job
				log.Printf("Assigning Map to worker: %s", worker)
				job_args = &DoJobArgs{mr.file, Map, i, mr.nReduce}
				ok = call(worker, "Worker.DoJob", job_args, &job_reply)
			}
			// When job done, set worker idle
			mr.Workers[worker].state = 0
		}()
		// blocks untill all giroutine in the wait list to finish
		wg.Wait()
	}

	// Start handling Reduce job
	for i := 0; i < mr.nReduce; i++ {
		// use loop to find an idel worker
		for {
			mr.mu.Lock()
			for _, w := range mr.Workers {
				if w.state == 0 {
					worker = w.address
					w.state = 1
					break
				}
			}
			mr.mu.Unlock()
			if worker != "" {
				// when we find a worker, break to assign job to this worker
				log.Printf("Find available worker: %s", worker)
				break
			}
		}
		// Let the worker do the Reduce job
		log.Printf("Assigning Reduce to worker: %s", worker)
		job_args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
		// add one more goroutine to the wait list
		wg.Add(1)
		go func() {
			defer wg.Done()
			var job_reply DoJobReply
			// RPC call, recursively calling as long as the job is not done
			ok := call(worker, "Worker.DoJob", job_args, &job_reply)
			for ok == false && job_reply.OK == false {
				mr.Workers[worker].state = -1
				log.Printf("Worker: %s failed, try to find a new one", worker)
				// use loop to find an idel worker
				for {
					mr.mu.Lock()
					for _, w := range mr.Workers {
						if w.state == 0 {
							worker = w.address
							w.state = 1
							break
						}
					}
					mr.mu.Unlock()
					if worker != "" {
						// when we find a worker, break to assign job to this worker
						log.Printf("Find available worker: %s", worker)
						break
					}
				}
				// Let the work do the Reduce job
				log.Printf("Assigning Reduce to worker: %s", worker)
				job_args = &DoJobArgs{mr.file, Reduce, i, mr.nMap}
				ok = call(worker, "Worker.DoJob", job_args, &job_reply)
			}
			// When job done, set worker idle
			mr.Workers[worker].state = 0
		}()
		// blocks untill all giroutine in the wait list to finish
		wg.Wait()
	}

	return mr.KillWorkers()
}
