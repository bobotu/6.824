package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
	"sync"
	"unsafe"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) error {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!

	debug("%s map job start\n", jobName)

	var wg sync.WaitGroup
	saveWorker := make([]chan KeyValue, nReduce)

	for i := range saveWorker {
		wg.Add(1)
		saveWorker[i] = make(chan KeyValue, 10)
		go func(i int, c <-chan KeyValue) {
			f, _ := os.OpenFile(reduceName(jobName, mapTaskNumber, i), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
			enc := json.NewEncoder(f)

			for kv := range c {
				if err := enc.Encode(kv); err != nil {
					debug("%s\n", err.Error())
				} else {
					debug("%d save one kv %v\n", i, kv)
				}
			}

			f.Close()
			debug("%s closed\n", reduceName(jobName, mapTaskNumber, i))
			wg.Done()
		}(i, saveWorker[i])
	}

	f, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	content := *((*string)(unsafe.Pointer(&b)))
	result := mapF(inFile, content)

	for _, kv := range result {
		r := ihash(kv.Key) % uint32(nReduce)
		saveWorker[r] <- kv
		debug("send %v\n", kv)
	}

	for _, c := range saveWorker {
		close(c)
	}

	wg.Wait()
	debug("%d done\n", mapTaskNumber)

	return nil
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
