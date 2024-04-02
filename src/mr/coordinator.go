package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type WorkerAssignment struct {
	Stage    string
	SeqId    int
}

type Coordinator struct {
	// User defined fields
	Files        []string

	
	// Internal state
	// Values for MapStates and ReduceStates:
	// *Pending: Task is not assigned to any worker.
	// *Assigned: Task is assigned to a worker but has not been acknowledged as completed.
	// *Done: Task is completed.
	MapStates      		sync.Map // map[string]string
	ReduceStates   		sync.Map // map[string]string
	// WorkersAssignments: WorkerId -> WorkerAssignment
	WorkerAssignments sync.Map // map[int]WorkerAssignment
	Heartbeats			  sync.Map // map[int]time.Time
	// SeqId represents the sequence id of the file that is being processed.
	// For Map tasks, it represents the index of the file in Files.
	// For Reduce tasks, it represents the index of the reduce task.
	SeqId   			 int
	NReduce 			 int
	// In the case of a worker failure, we need to reassign the task to another worker.
	// Which we can do with a queue of SeqIds and the stage they belong to.
	// In almost all case it is safe to assume the stage of the backlog is the same as the current stage.
	// The only exception is when we are in the "Wait" stage, where we need to wait for all map tasks to complete.
	// In this case, we assume the stage is "Map" since "Reduce" automatically exit
	// when there are no more reduce tasks to assign and the state machine never
	// transitions to "Wait" after the Map stage.
	Backlog 			 []WorkerAssignment
	
	// Private fields
	Lock 				 	 sync.RWMutex
	logger				*log.Logger
	nextWorkerId 	 int
	running 			 bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// Prioritize the backlog.
	if len(c.Backlog) > 0 {
		stage, seqId := c.Backlog[0].Stage, c.Backlog[0].SeqId
		c.Backlog = c.Backlog[1:] // Pop the first element

		c.logger.Printf("Assigning task <workerId:%d stage:%s seqId:%d> from backlog", args.WorkerId, stage, seqId)

		return c.AssignTask(args.WorkerId, stage, seqId, reply)
	}

	c.Lock.Lock()
	stage, seqId := c.CurrentStage()

	c.logger.Printf("State machine says <workerId:%d stage:%s seqId:%d>", args.WorkerId, stage, seqId)

	if stage == "Exit" {
		if len(c.Backlog) != 0 {
			c.logger.Fatalf("received exit signal but backlog is not empty")
		}
		c.RequestShutdown()
	} else if stage == "Map" || stage == "Reduce" {
		// Step the sequence id if we are in the Map or Reduce stage.
		c.SeqId++
	}

	c.Lock.Unlock()
	return c.AssignTask(args.WorkerId, stage, seqId, reply)
}

func (c *Coordinator) ReportTaskCompletion(args *ReportTaskCompletionArgs, reply *ReportTaskCompletionReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()


	if args.Stage == "Map" {
		// if c.MapStates[args.Filename] != "MapAssigned" {
		value, ok := c.MapStates.Load(args.Filename)
		if !ok || value.(string) != "MapAssigned" {
			return fmt.Errorf("map task %s is not assigned or already completed", args.Filename)
		}
		c.MapStates.Store(args.Filename, "MapDone")
	} else if args.Stage == "Reduce" {
		value, ok := c.ReduceStates.Load(args.Filename)
		if !ok || value.(string) != "ReduceAssigned" {
			return fmt.Errorf("reduce task %s is not assigned or already completed", args.Filename)
		}
		c.ReduceStates.Store(args.Filename, "ReduceDone")
	} else {
		return fmt.Errorf("invalid stage %s", args.Stage)
	}

	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	
	reply.WorkerId = c.NextWorkerId()
	assignment := WorkerAssignment{
		Stage: "Unassigned",
		SeqId: -1,
	}
	// c.WorkerAssignments[reply.WorkerId] = assignment
	c.WorkerAssignments.Store(reply.WorkerId, assignment)

	c.logger.Printf("registering worker %d", reply.WorkerId)

	return nil
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	// c.Heartbeats[args.WorkerId] = time.Now()
	c.Heartbeats.Store(args.WorkerId, time.Now())
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		c.logger.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	c.logger.Printf("server started on <sock:%s>", sockname)
}

// RequestShutdown sets the running flag to false, which will
// gracefully shutdown the coordinator after suspending all workers.
func (c *Coordinator) RequestShutdown() {

	c.running = false
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Returns true if the job is done, false otherwise.
	c.Lock.RLock()
	defer c.Lock.RUnlock()

	return !c.running
}

// Retrieve next worker id and increment the counter.
// TODO: This is not thread safe. Use mutex to protect this.
func (c *Coordinator) NextWorkerId() int {
	c.nextWorkerId++
	return c.nextWorkerId
}

func (c *Coordinator) CurrentStage() (string, int) {
	// Returns the current stage of the job.
	// Map, Reduce, Wait, Exit
	// If the job is done, it returns "Exit".
	// Caller must ensure that a read lock is held on the coordinator
	// at a minimum before calling this function.

	if c.SeqId < len(c.Files) {
		return "Map", c.SeqId
	} else if c.SeqId == len(c.Files) {
		// We need to wait for all map tasks to complete until we can start reduce tasks.
		mapFreq := make(map[string]int)
		c.MapStates.Range(func(key, value interface{}) bool {
			mapFreq[value.(string)]++
			return true
		})

		if mapFreq["MapDone"] == len(c.Files) {
			// All map tasks are done, we can start reduce tasks.
			c.logger.Printf("all map tasks are done, starting reduce tasks")
			return "Reduce", 0
		}
		c.logger.Printf("waiting for map tasks to complete")
		return "Wait", -1

	} else if c.SeqId > len(c.Files) && c.SeqId < len(c.Files)+c.NReduce {
		// Must be in the Reduce stage.
		return "Reduce", c.SeqId - len(c.Files)
	} else if c.SeqId == len(c.Files)+c.NReduce {
		// We need to make sure all reduce tasks are done before we can exit.
		reduceFreq := make(map[string]int)
		c.ReduceStates.Range(func(key, value interface{}) bool {
			reduceFreq[value.(string)]++
			return true
		})

		if reduceFreq["ReduceDone"] == c.NReduce {
			// All reduce tasks are done, we can exit.
			c.logger.Printf("all reduce tasks are done, exiting")
			return "Exit", -1
		}
		c.logger.Printf("waiting for reduce tasks to complete")
		return "Wait", -1
	}
		

	// By default, we are in the Exit stage.
	return "Exit", -1
}

// Modifies coordinator state to assign a task to a worker,
// expects arguments to already be normalized.
// Caller must ensure that the lock on the coordinator is held.
func (c *Coordinator) AssignTask(workerId int, stage string, seqId int, reply *RequestTaskReply) error {
	c.logger.Printf("Assigning task <workerId:%d stage:%s seqId:%d>", workerId, stage, seqId)

	if stage == "Map" {
		currentFileName := c.Files[seqId]
		// Panic if we try to assign a task that is not pending.
		value, ok := c.MapStates.Load(currentFileName)
		if !ok || value.(string) != "MapPending"{
			return fmt.Errorf("map task %s is not pending", currentFileName)
		}
		c.MapStates.Store(currentFileName, "MapAssigned")

		reply.Filename = c.Files[seqId]
		reply.SequenceId = seqId
		reply.Stage = "Map"
	} else if stage == "Reduce" {
		filename := "mr-out-" + strconv.Itoa(seqId)

		value, ok := c.ReduceStates.Load(filename)
		if !ok || value.(string) != "ReducePending" {
			return fmt.Errorf("reduce task %s is not pending", filename)
		}
		// c.ReduceStates[filename] = "ReducePending"
		c.ReduceStates.Store(filename, "ReduceAssigned")

		reply.Filename = filename
		reply.SequenceId = seqId
		reply.Stage = "Reduce"
	} else if stage == "Wait" {
		reply.Stage = "Wait"
	} else if stage == "Exit" {
		reply.Stage = "Exit"
	} else {
		return fmt.Errorf("invalid stage %s", stage)
	}

	// Assign the task to the worker.
	assignment := WorkerAssignment{
		Stage: stage,
		SeqId: seqId,
	}
	// c.WorkerAssignments[workerId] = assignment
	c.WorkerAssignments.Store(workerId, assignment)
	reply.NumReduceTasks = c.NReduce
	
	return nil
}

func (c *Coordinator) LogState() {
	c.Lock.RLock()
	defer c.Lock.RUnlock()

	// Create a copy of the MapStates before calling logger.Printf
	mapStatesCopy := make(map[string]string)
	c.MapStates.Range(func(key, value interface{}) bool {
		mapStatesCopy[key.(string)] = value.(string)
		return true
	})

	// Create a copy of the ReduceStates before calling logger.Printf
	reduceStatesCopy := make(map[string]string)
	c.ReduceStates.Range(func(key, value interface{}) bool {
		reduceStatesCopy[key.(string)] = value.(string)
		return true
	})

	// Create a copy of the WorkerAssignments before calling logger.Printf
	workerAssignmentsCopy := make(map[int]WorkerAssignment)
	c.WorkerAssignments.Range(func(key, value interface{}) bool {
		workerAssignmentsCopy[key.(int)] = value.(WorkerAssignment)
		return true
	})

	// Create a copy of the Heartbeats before calling logger.Printf
	heartbeatsCopy := make(map[int]time.Time)
	c.Heartbeats.Range(func(key, value interface{}) bool {
		heartbeatsCopy[key.(int)] = value.(time.Time)
		return true
	})

	c.logger.Printf("MapStates: %v", mapStatesCopy)
	c.logger.Printf("ReduceStates: %v", reduceStatesCopy)
	c.logger.Printf("WorkerAssignments: %v", workerAssignmentsCopy)
	c.logger.Printf("Heartbeats: %v", heartbeatsCopy)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:   files,
		SeqId:   0,
		NReduce: nReduce,
		MapStates:  sync.Map{},
		ReduceStates: sync.Map{},
		WorkerAssignments: sync.Map{},
		Heartbeats: sync.Map{},
		Backlog: make([]WorkerAssignment, 0),
		running: true,
	}

	// Init logger
	logFile, err := os.OpenFile("coordinator.log", os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("cannot open log file")
	}

	c.logger = log.New(logFile, "[Coordinator] ", log.LstdFlags)

	for _, file := range files {
		c.MapStates.Store(file, "MapPending")
	}

	for i := 0; i < nReduce; i++ {
		filename := "mr-out-" + strconv.Itoa(i)
		c.ReduceStates.Store(filename, "ReducePending")
	}

	c.LogState()

	// Monitor heartbeats to ensure no workers are stale (possibly dead).
	go func() {
		for {
			c.Heartbeats.Range(func(key, value interface{}) bool {
				workerId := key.(int)
				lastHeartbeat := value.(time.Time)
				if time.Since(lastHeartbeat) > 10*time.Second {
					value, ok := c.WorkerAssignments.Load(workerId)
					if !ok {
						c.logger.Printf("Worker %d is stale, but no assignment found", workerId)
					}
					assignment := value.(WorkerAssignment)
					c.logger.Printf("Worker %d <task seqId:%d stage:%v> is stale, removing...", workerId, assignment.SeqId, assignment.Stage)

					// If the worker is assigned a task, we need to reassign it.
					if assignment.Stage != "Unassigned" {
						c.Backlog = append(c.Backlog, assignment)
						c.logger.Printf("Reassigning task <workerId:%d stage:%s seqId:%d> to backlog", workerId, assignment.Stage, assignment.SeqId)
						switch assignment.Stage {
						case "Map":
							// c.MapStates[c.Files[assignment.SeqId]] = "MapPending"
							c.MapStates.Store(c.Files[assignment.SeqId], "MapPending")
						case "Reduce":
							// c.ReduceStates["mr-out-" + strconv.Itoa(assignment.SeqId)] = "ReducePending"
							c.ReduceStates.Store("mr-out-" + strconv.Itoa(assignment.SeqId), "ReducePending")
						}
					} else {
						c.logger.Printf("No task assigned to worker %d", workerId)
					}
					c.WorkerAssignments.Delete(workerId)
					c.Heartbeats.Delete(workerId)
				}
				return true
			})
			time.Sleep(1 * time.Second)
		}
	}()

	c.server()
	return &c
}
