package mapreduce

import "container/list"
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
	jobDoneChannel := make(chan int)

	for i := 0; i < mr.nMap; i++ {
		go func(jobNumber int) {
			for {
				worker := <-mr.idleWorkerChannel
				jobArgs := &DoJobArgs{mr.file, Map, jobNumber, mr.nReduce}
				jobReply := &DoJobReply{}
				ok := call(worker, "Worker.DoJob", jobArgs, jobReply)
				if ok == true {
					mr.idleWorkerChannel <- worker
					jobDoneChannel <- jobNumber
					return
				}
			}
		}(i)
	}

	for i := 0; i < mr.nMap; i++ {
		<-jobDoneChannel
	}

	for i := 0; i < mr.nReduce; i++ {
		go func(jobNumber int) {
			for {
				worker := <-mr.idleWorkerChannel
				jobArgs := &DoJobArgs{mr.file, Reduce, jobNumber, mr.nMap}
				jobReply := &DoJobReply{}
				ok := call(worker, "Worker.DoJob", jobArgs, jobReply)
				if ok == true {
					mr.idleWorkerChannel <- worker
					jobDoneChannel <- jobNumber
					return
				}
			}
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-jobDoneChannel
	}

	return mr.KillWorkers()
}
