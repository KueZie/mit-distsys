package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// CrashSafeFile is a wrapper around os.File that ensures that the file 
// is only visible to other processes when it is fully written.
// This process ensures correct task consistency in the event of a
// worker crash.
// The file is first written to a temporary file, and then renamed to the final filename
// when it is closed. There may be some overhead since Close() is forced to sync the file
// before renaming it.
type CrashSafeFile struct {
	filename string
	File     *os.File
}

// Write implements io.Writer.
func (f CrashSafeFile) Write(p []byte) (n int, err error) {
	return f.File.Write(p)
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (f *CrashSafeFile) Open(filename string) error {
	tmpFilename := fmt.Sprintf("%s.tmp", filename)
	file, err := os.OpenFile(tmpFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	f.filename = filename
	f.File = file
	return nil
}

func (f *CrashSafeFile) Close() error {
	// When closing the file, we rename the temporary file to the final filename
	// This is to ensure that the file is only visible to other processes when it is fully written

	if err:= f.File.Close(); err != nil {
		return err
	}

	// Rename the file
	return os.Rename(fmt.Sprintf("%s.tmp", f.filename), f.filename)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var logger *log.Logger

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Register worker
	workerId, err := CallRegisterWorker()
	if err != nil {
		log.Fatalf("call to RegisterWorker failed")
	}

	logFileName := fmt.Sprintf("coordinator-worker-%d.log", workerId)
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("cannot open log file")
	}

	// Setup logger with prefix
	logger = log.New(logFile, fmt.Sprintf("[Worker %d] ", workerId), log.LstdFlags)

	// Notify coordinator of heartbeat
	go func() {
		for {
			err := CallNotifyHeartbeat(workerId)
			if err != nil {
				logger.Fatalf("call to NotifyHeartbeat failed: %v", err)
			}

			// Sleep before sending the next heartbeat
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// Request tasks from coordinator and execute them
	exit := make(chan bool)
	go func() {
		for {
			reply, err := CallRequestTask(workerId)
			if err != nil {
				logger.Fatalf("call to RequestTask failed")
			}
			requestShutdown := RequestProcess(workerId, mapf, reducef, reply)
			if requestShutdown {
				exit <- true
				break
			}
		}
	}()

	// Wait for exit signal
	<-exit
}

// RequestProcessLoop is a loop that processes tasks received from the coordinator
// It processes Map and Reduce tasks
// Returns true when the coordinator sends an Exit task
func RequestProcess(workerId int, mapf func(string, string) []KeyValue, reducef func(string, []string) string, reply RequestTaskReply) bool {

	// If the coordinator sends an Exit task, we should exit
	if reply.Stage == "Exit" {
		logger.Printf("Received Exit task, shutting down\n")
		return true
	}

	logger.Printf("Received task %s\n", reply.Stage)
	switch reply.Stage {
	case "Map":
		// read each input file,
		// pass it to Map,
		// accumulate the intermediate Map output.
		intermediate := []KeyValue{}
		file, err := os.Open(reply.Filename)
		if err != nil {
			logger.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			logger.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()
		kva := mapf(reply.Filename, string(content))
		intermediate = append(intermediate, kva...)

		// Write intermediate to disk mr-SeqId-ReduceSeqId
		numWrites := 0
		// for _, kv := range intermediate {
		// 	// Choose the ReduceSeqId using a hash function has the property of
		// 	// grouping the same key to the same ReduceSeqId
		// 	reduceSeqId := ihash(kv.Key) % reply.NumReduceTasks
		// 	filename := fmt.Sprintf("mr-%d-%d", reply.SequenceId, reduceSeqId)
		// 	// Create the file if it doesn't exist, append to it if it does, write only
		// 	file := CrashSafeFile{}
		// 	err := file.Open(filename)
		// 	if err != nil {
		// 		logger.Fatalf("cannot open %v", filename)
		// 	}
		// 	out, err := fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
		// 	if err != nil {
		// 		logger.Fatalf("cannot write to %v", filename)
		// 	}
		// 	totalBytes += out

		mapOutput := make(map[int][]KeyValue)
		for _, kv := range intermediate {	
			bucket := ihash(kv.Key) % reply.NumReduceTasks
			mapOutput[bucket] = append(mapOutput[bucket], kv)
		}

		for bucket, kva := range mapOutput {
			filename := fmt.Sprintf("mr-%d-%d.json", reply.SequenceId, bucket)
			file := CrashSafeFile{}
			err := file.Open(filename)
			if err != nil {
				logger.Fatalf("cannot open %v", filename)
			}
			// Encode the key-value pairs to JSON
			jsonFile := json.NewEncoder(file)
			err = jsonFile.Encode(kva)
			if err != nil {
				logger.Fatalf("cannot write to %v", filename)
			}
			numWrites += 1
			// Close the file to ensure that the file is only visible to other processes
			defer file.Close()
		}

		logger.Printf("Wrote %d times to disk for task: %s\n", numWrites, reply.Filename)

		// Report task completion
		logger.Printf("reporting map task %d completed\n", reply.SequenceId)
		err = CallReportTaskCompletion("Map", reply.Filename)
		if err != nil {
			logger.Fatalf("call to ReportTaskCompletion failed")
		}
	case "Reduce":
		// Read intermediate files from disk
		// Call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		logger.Printf("Reduce task %d\n", reply.SequenceId)
		intermediate := []KeyValue{}
		for i := 0; i < 8; i++ {
			filename := fmt.Sprintf("mr-%d-%d.json", i, reply.SequenceId)
			file, err := os.Open(filename)
			if err != nil {
				logger.Printf("cannot open %v possible never created - skipping\n", filename)
				continue
			}

			fileOutput := []KeyValue{}

			// Decode the JSON file
			jsonFile := json.NewDecoder(file)
			err = jsonFile.Decode(&fileOutput)
			if err != nil {
				logger.Fatalf("cannot decode %v", filename)
			}

			intermediate = append(intermediate, fileOutput...)
		}

			// It is possible that the file does not exist because the map task
			// did not emit any key-value pairs that hash to this reduce task
			// In this case, we skip the file, but still report the task as completed

		// 	if err != nil {
		// 		logger.Printf("cannot open %v possible 'Map' no init bucket\n", filename)
		// 		continue
		// 	}

		// 	bufioReader := bufio.NewReader(file)

		// 	content := []string{}
		// 	for {
		// 		line, err := bufioReader.ReadString('\n')
		// 		if err != nil {
		// 			break
		// 		}
		// 		content = append(content, line)
		// 	}

		// 	kva := []KeyValue{}
		// 	for _, line := range content {
		// 		kv := KeyValue{}
		// 		n, err := fmt.Sscanf(string(line), "%s %s", &kv.Key, &kv.Value)
		// 		if err != nil || n != 2 {
		// 			logger.Fatalf("cannot parse %v on line %v in %v", line, n, filename)
		// 		}
		// 		kva = append(kva, kv)
		// 	}

		// 	intermediate = append(intermediate, kva...)

		// 	file.Close()
		// }

		// Sort intermediate by key
		sort.Sort(ByKey(intermediate))

		logger.Printf("read %d key-value pairs from all files\n", len(intermediate))

		// Write output to disk
		oname := reply.Filename
		ofile, _ := os.Create(oname)
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				// Count occurrences other than self
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		logger.Printf("Wrote output to %s\n", oname)

		ofile.Close()
		// Report task completion
		err := CallReportTaskCompletion("Reduce", oname)
		if err != nil {
			logger.Fatalf("call to ReportTaskCompletion failed")
		}
	case "Wait":
		logger.Printf("Received Wait task, waiting for more tasks\n")
		time.Sleep(1 * time.Second)
	default:
		logger.Fatalf("Invalid stage %s", reply.Stage)
	}

	return false // Continue processing tasks
}

func CallRequestTask(workerId int) (RequestTaskReply, error) {
	args := RequestTaskArgs{
		WorkerId: workerId,
	}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		// fmt.Printf("<%s> %s %d\n", reply.Stage, reply.Filename, reply.SequenceId)
	} else {
		logger.Printf("call failed!\n")
		return reply, fmt.Errorf("call failed")
	}

	return reply, nil
}

func CallReportTaskCompletion(stage string, filename string) error {
	args := ReportTaskCompletionArgs{Stage: stage, Filename: filename}
	reply := ReportTaskCompletionReply{}

	ok := call("Coordinator.ReportTaskCompletion", &args, &reply)
	if ok {
		logger.Printf("reported task <stage:%s filename:%s> complete\n", stage, filename)
	} else {
		fmt.Printf("call failed!\n")
		return fmt.Errorf("call failed")
	}

	return nil
}

func CallRegisterWorker() (int, error) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		// fmt.Printf("registered worker\n")
	} else {
		logger.Printf("call failed!\n")
		return 0, fmt.Errorf("call failed")
	}

	return reply.WorkerId, nil
}

func CallNotifyHeartbeat(workerId int) error {
	args := HeartbeatArgs{
		WorkerId: workerId,
	}
	reply := HeartbeatReply{}

	ok := call("Coordinator.Heartbeat", &args, &reply)
	if ok {
		// logger.Printf("received hearbeat response\n")
	} else {
		return fmt.Errorf("heartbeat call failed")
	}

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		logger.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	logger.Println(err)
	return false
}
