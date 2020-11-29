package maple_juice_service

import (
	"better_mp3/app/file_service"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/util/sets"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type MapleJuiceServer struct {
	config     MJConf
	fileServer *file_service.FileServer
}

func NewMapleJuiceServer(fileServer *file_service.FileServer) MapleJuiceServer {
	var f MapleJuiceServer
	f.config = NewMJConf()
	f.fileServer = fileServer
	return f
}

func (mjServer MapleJuiceServer) RunMapleTask(args map[string]string, mapleResult *string) error {
	inputFile := args["input"]
	application := args["application"]
	outputPrefix := args["output_prefix"]

	//fmt.Println("Get application from DFS")
	// Get application executable from DFS
	mjServer.fileServer.Get(application, path.Join(mjServer.config.AppPath, application))

	//fmt.Println("Get input from DFS")
	// Get input file from DFS
	mjServer.fileServer.Get(inputFile, path.Join(mjServer.config.AppPath, inputFile))

	//fmt.Println("Call maple function")
	// Call maple function (10 lines at a time)
	f, err := os.Open(path.Join(mjServer.config.AppPath, inputFile))
	if err != nil {
		log.Fatalln(err)
	}
	reader := bufio.NewReader(f)
	for {
		var content []string
		for i := 0; i < 10; i++ {
			str, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			content = append(content, str)
		}
		if len(content) == 0 {
			break
		}
		cmd := exec.Command(path.Join(mjServer.config.AppPath, application), strings.Join(content, "\n"))
		ret, err := cmd.CombinedOutput()
		if err != nil {
			log.Println("Application error: ", err)
			return err
		}

		//fmt.Println("Get maple results")
		// Get resulting key-value pairs
		var results []string
		err = json.Unmarshal(ret, &results)
		if err != nil {
			log.Println(err)
		}
		*mapleResult = strings.Join(results, "\n")

		//fmt.Println("Write maple results")
		// Append intermediate result to DFS
		reg, err := regexp.Compile("[^a-zA-Z0-9]+")
		if err != nil {
			log.Fatalln(err)
		}
		for _, pair := range results {
			tmp := strings.Split(pair, ",")
			key := reg.ReplaceAllString(tmp[0], "")
			mjServer.fileServer.Append(pair + "\n", outputPrefix+"_"+key)
			//time.Sleep(time.Millisecond * 200)
		}
	}
	if err := f.Close(); err != nil {
		log.Fatalln(err)
	}

	return nil
}

func (mjServer MapleJuiceServer) RunJuiceTask(args map[string]string, juiceResult *string) error {
	inputFile := args["input"]
	application := args["application"]

	//fmt.Println("Get application from DFS")
	// Get application executable from DFS
	mjServer.fileServer.Get(application, path.Join(mjServer.config.AppPath, application))

	//fmt.Println("Get input from DFS")
	// Get input file from DFS
	mjServer.fileServer.Get(inputFile, path.Join(mjServer.config.AppPath, inputFile))

	time.Sleep(time.Second)

	//fmt.Println("Call juice function")
	// Call juice function
	var ret []byte
	var err error
	for {
		cmd := exec.Command(path.Join(mjServer.config.AppPath, application), path.Join(mjServer.config.AppPath, inputFile))
		ret, err = cmd.CombinedOutput()
		if err == nil {
			break
		}
		log.Println(err)
	}

	//fmt.Println("Get juice results")
	// Get the resulting key-value pair
	*juiceResult = string(ret)
	return nil
}

func (mjServer *MapleJuiceServer) ScheduleMapleTask(cmd []string) {
	start := time.Now().UnixNano() / int64(time.Millisecond)

	application := cmd[1]
	taskNum, _ := strconv.Atoi(cmd[2])
	filenamePrefix := cmd[3]
	inputDir := cmd[4]

	fmt.Println("Starting partitioning")
	// Partition input data (hash partitioning)
	outFiles := map[string]*os.File{}
	for i := 0; i < taskNum; i++ {
		inputFile := path.Join(inputDir, strconv.Itoa(i))
		f, err := os.OpenFile(inputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		outFiles["input"+strconv.Itoa(i)] = f
		if err != nil {
			log.Fatal(err)
		}
	}
	file, err := os.Open(path.Join(inputDir, mjServer.config.InputFile))
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(file)
	var line string
	lineNum := 1
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		if _, err = outFiles["input"+strconv.Itoa(lineNum%taskNum)].WriteString(line); err != nil {
			log.Fatal(err)
		}
		lineNum++
	}
	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}
	for i := 0; i < taskNum; i++ {
		if err := outFiles["input"+strconv.Itoa(i)].Close(); err != nil {
			log.Fatalln(err)
		}
	}
	if err := file.Close(); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Done partitioning")

	fmt.Println("Start scheduling")
	// Schedule tasks (in turn)
	mjServer.fileServer.Put(application, application)
	tasks := map[string]string{}
	it := mjServer.fileServer.FileTable.Storage.Iterator()
	for i := 0; i < taskNum; i++ {
		inputFile := path.Join(inputDir, strconv.Itoa(i))
		mjServer.fileServer.Put(inputFile, strconv.Itoa(i))
		if it.Next() == false {
			it.First()
		}
		node := it.Value()
		tasks[strconv.Itoa(i)] = node.(file_service.FileTableEntry).ServerIP
	}
	fmt.Println("Done scheduling")

	fmt.Println("Start RPC")
	// Asynchronous RPC
	port := mjServer.config.Port
	var calls []RPCTask
	var unfinishedTasks []string
	var failedIP []string
	mapleResults := make([]string, len(tasks))
	cnt := 0
	for inputFile, ip := range tasks {
		client, err := rpc.Dial("tcp", ip+":"+port)
		if err != nil {
			log.Println("Need rescheduling:  ", err)
			unfinishedTasks = append(unfinishedTasks, inputFile)
			failedIP = append(failedIP, ip)
			continue
		}
		args := map[string]string{
			"input":         inputFile,
			"application":   application,
			"output_prefix": filenamePrefix,
		}
		calls = append(calls, RPCTask{inputFile, ip, *client.Go("MapleJuiceRPCServer.Maple", args, &mapleResults[cnt], nil)})
		cnt++
	}

	// Synchronization
	for _, tmp := range calls {
		replyCall := <-tmp.call.Done
		if replyCall.Error != nil {
			log.Println("Need rescheduling:  ", replyCall.Error)
			unfinishedTasks = append(unfinishedTasks, tmp.fileName)
			failedIP = append(failedIP, tmp.ip)
			continue
		}
	}

	// Reschedule unfinished tasks
	tasks = map[string]string{}
	it = mjServer.fileServer.FileTable.Storage.Iterator()
	for i := 0; i < len(unfinishedTasks); i++ {
		if it.Next() == false {
			it.First()
		}
		node := it.Value()
		for _, ip := range failedIP {
			if node.(file_service.FileTableEntry).ServerIP == ip {
				if it.Next() == false {
					it.First()
				}
				node = it.Value()
			}
		}
		tasks[unfinishedTasks[i]] = node.(file_service.FileTableEntry).ServerIP
	}
	var newCalls []rpc.Call
	newResults := make([]string, len(unfinishedTasks))
	cnt = 0
	for inputFile, ip := range tasks {
		client, err := rpc.Dial("tcp", ip+":"+port)
		if err != nil {
			log.Fatal(err)
		}
		args := map[string]string{
			"input":         inputFile,
			"application":   application,
			"output_prefix": filenamePrefix,
		}
		newCalls = append(newCalls, *client.Go("MapleJuiceRPCServer.Maple", args, &newResults[cnt], nil))
		cnt++
	}

	for _, call := range newCalls {
		replyCall := <-call.Done
		if replyCall.Error != nil {
			log.Fatalln("Reschedule failed: ", replyCall.Error)
		}
	}
	fmt.Println("Done RPC")

	end := time.Now().UnixNano() / int64(time.Millisecond)
	log.Println("Maple elapsed", end - start, "milliseconds.")
}

func (mjServer *MapleJuiceServer) ScheduleJuiceTask(cmd []string) {
	start := time.Now().UnixNano() / int64(time.Millisecond)

	application := cmd[1]
	taskNum, _ := strconv.Atoi(cmd[2])
	filenamePrefix := cmd[3]
	output := cmd[4]

	fmt.Println("Start searching")
	// Find intermediate files
	files := mjServer.fileServer.FileTable.ListFilesByPrefix(filenamePrefix)
	fmt.Println("Done searching")

	fmt.Println("Start scheduling")
	// Schedule tasks (in turn)
	mjServer.fileServer.Put(application, application)
	var tasks []map[string]string
	for i := 0; i < taskNum; i++ {
		tasks = append(tasks, map[string]string{})
	}
	it := mjServer.fileServer.FileTable.Storage.Iterator()
	for i, filename := range files {
		if it.Next() == false {
			it.First()
		}
		node := it.Value()
		tasks[i%taskNum][filename] = node.(file_service.FileTableEntry).ServerIP
	}
	fmt.Println("Done scheduling")

	fmt.Println("Start RPC")
	// Asynchronous RPC
	port := mjServer.config.Port
	var calls []RPCTask
	var unfinishedTasks []string
	var failedIP []string
	juiceResults := make([]string, len(files))
	cnt := 0
	for _, m := range tasks {
		for inputFile, ip := range m {
			client, err := rpc.Dial("tcp", ip+":"+port)
			if err != nil {
				log.Println("Need rescheduling:  ", err)
				unfinishedTasks = append(unfinishedTasks, inputFile)
				failedIP = append(failedIP, ip)
				continue
			}
			args := map[string]string{
				"input":       inputFile,
				"application": application,
			}
			calls = append(calls, RPCTask{inputFile, ip, *client.Go("MapleJuiceRPCServer.Juice", args, &juiceResults[cnt], nil)})
			cnt++
		}
	}

	// Synchronization
	for _, tmp := range calls {
		replyCall := <-tmp.call.Done
		if replyCall.Error != nil {
			log.Println("Need rescheduling:  ", replyCall.Error)
			unfinishedTasks = append(unfinishedTasks, tmp.fileName)
			failedIP = append(failedIP, tmp.ip)
			continue
		}
	}

	// Reschedule unfinished tasks
	var newTasks []map[string]string
	for i := 0; i < len(unfinishedTasks); i++ {
		newTasks = append(newTasks, map[string]string{})
	}
	it = mjServer.fileServer.FileTable.Storage.Iterator()
	for i, filename := range unfinishedTasks {
		if it.Next() == false {
			it.First()
		}
		node := it.Value()
		for _, ip := range failedIP {
			if node.(file_service.FileTableEntry).ServerIP == ip {
				if it.Next() == false {
					it.First()
				}
				node = it.Value()
			}
		}
		newTasks[i%len(unfinishedTasks)][filename] = node.(file_service.FileTableEntry).ServerIP
	}
	var newCalls []rpc.Call
	newResults := make([]string, len(unfinishedTasks))
	cnt = 0
	for _, m := range newTasks {
		for inputFile, ip := range m {
			client, err := rpc.Dial("tcp", ip+":"+port)
			if err != nil {
				log.Fatal(err)
			}
			args := map[string]string{
				"input":       inputFile,
				"application": application,
			}
			newCalls = append(newCalls, *client.Go("MapleJuiceRPCServer.Juice", args, &newResults[cnt], nil))
			cnt++
		}
	}
	for _, call := range newCalls {
		replyCall := <-call.Done
		if replyCall.Error != nil {
			log.Fatalln("Reschedule failed ", replyCall.Error)
		}
	}
	fmt.Println("Done RPC")

	fmt.Println("Start sorting")
	// Sort results and write to DFS
	var results []string
	for _, s := range juiceResults {
		if s != "" {
			results = append(results, s)
		}
	}
	for _, s := range newResults {
		if s != "" {
			results = append(results, s)
		}
	}
	sortedResults := sets.NewString()
	for _, kvPair := range results {
		sortedResults.Insert(kvPair)
	}
	content := strings.Join(sortedResults.List(), "\n") + "\n"
	mjServer.fileServer.Append(content, output)
	fmt.Println("Done sorting")

	// Delete intermediate files
	if len(cmd) == 6 && cmd[5] == "1" {
		for _, file := range files {
			mjServer.fileServer.Delete(file)
		}
	}

	end := time.Now().UnixNano() / int64(time.Millisecond)
	log.Println("Juice elapsed", end - start, "milliseconds.")
}