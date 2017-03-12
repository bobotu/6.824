package mapreduce

import "fmt"

type task struct {
	file   string
	number int
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	counter := make(chan int)

	tasks := make(chan *task, ntasks)
	for i := 0; i < ntasks; i++ {
		var file string
		if phase == mapPhase {
			file = mr.files[i]
		}
		tasks <- &task{file: file, number: i}
	}

	mr.Lock()
	for _, wk := range mr.workers {
		go mr.doTask(wk, tasks, counter, phase, nios)
	}
	mr.Unlock()

	for count := 0; count < ntasks; {
		select {
		case number := <-counter:
			debug("%s work %d done\n", phase, number)
			count++
		case wk := <-mr.registerChannel:
			mr.workers = append(mr.workers, wk)
			go mr.doTask(wk, tasks, counter, phase, nios)
		}
	}

	close(tasks)

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func (mr *Master) doTask(wk string, tasks chan *task, counter chan<- int, phase jobPhase, nios int) {
	arg := new(DoTaskArgs)
	for t := range tasks {
		arg.JobName = mr.jobName
		arg.NumOtherPhase = nios
		arg.Phase = phase
		arg.TaskNumber = t.number
		arg.File = t.file

		if call(wk, "Worker.DoTask", arg, new(struct{})) {
			counter <- t.number
		} else {
			fmt.Printf("%s call fail\n", wk)
			tasks <- t
		}
	}
}
