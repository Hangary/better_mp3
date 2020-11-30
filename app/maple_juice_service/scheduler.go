package maple_juice_service

import (
	"better_mp3/app/file_service"
	"better_mp3/app/logger"
	"bufio"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/util/sets"
	"log"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)
func getOutputFileName(outputPrefix string, taskIndex int) string {
	return outputPrefix + "-" + strconv.Itoa(taskIndex)
}

func (mjServer *MapleJuiceServer) HashBasedPartition(inputFileName string, outputPrefix string, taskNum int) {
	logger.PrintInfo("Start partitioning")
	// Partition input data (hash partitioning)

	// open output files
	outputFiles := map[int]*os.File{}
	for i := 0; i < taskNum; i++ {
		outputFile := path.Join(mjServer.config.InputDir, getOutputFileName(outputPrefix, i))
		f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		outputFiles[i] = f
		if err != nil {
			log.Fatal(err)
		}
	}

	// open input file
	inputFile, err := os.Open(path.Join(mjServer.config.InputDir, inputFileName))
	if err != nil {
		log.Fatal(err)
	}
	inputFileReader := bufio.NewReader(inputFile)
	var line string
	lineNum := 1
	for {
		line, err = inputFileReader.ReadString('\n')
		if err != nil {
			break
		}
		if _, err = outputFiles[lineNum % taskNum].WriteString(line); err != nil {
			log.Fatal(err)
		}
		lineNum++
	}
	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}

	// close all output files
	for i := 0; i < taskNum; i++ {
		if err := outputFiles[i].Close(); err != nil {
			log.Fatalln(err)
		}
	}
	// close input file
	if err := inputFile.Close(); err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Done partitioning")
}


func (mjServer *MapleJuiceServer) ScheduleMapleTask(cmd []string) {
	logger.PrintInfo("Start scheduling maple task...")
	start := time.Now().UnixNano() / int64(time.Millisecond)

	execFileName := cmd[1]
	executableFilePath := path.Join(mjServer.config.ExecDir, execFileName)
	taskNum, _ := strconv.Atoi(cmd[2])
	outputPrefix := cmd[3]
	inputFileName := cmd[4]

	mjServer.HashBasedPartition(inputFileName, outputPrefix, taskNum)

	fmt.Println("Start scheduling")
	// Schedule mapleTasks (in turn)
	mjServer.fileServer.RemotePut(executableFilePath, execFileName)
	mapleTasks := map[string]string{} // taskNum -> serverIP
	it := mjServer.fileServer.FileTable.Storage.Iterator()
	for i := 0; i < taskNum; i++ {
		// upload partitioned input file to sdfs
		fileClip := path.Join(mjServer.config.InputDir, getOutputFileName(outputPrefix, i))
		mjServer.fileServer.RemotePut(fileClip, strconv.Itoa(i))

		if it.Next() == false {
			it.First()
		}
		node := it.Value()

		// assign task to one server
		mapleTasks[strconv.Itoa(i)] = node.(file_service.FileTableEntry).ServerIP
	}
	fmt.Println("Done scheduling")

	fmt.Println("Start RPC")
	// Asynchronous RPC
	port := mjServer.config.Port
	var calls []RPCTask
	var unfinishedTasks []string
	var failedIP []string
	mapleResults := make([]string, len(mapleTasks))
	cnt := 0
	for inputFile, ip := range mapleTasks {
		fmt.Println("DEBUG: Dialing RPC for", inputFile, ip)
		client, err := rpc.Dial("tcp", ip+":"+port)
		if err != nil {
			log.Println("Need rescheduling:  ", err)
			unfinishedTasks = append(unfinishedTasks, inputFile)
			failedIP = append(failedIP, ip)
			continue
		}

		calls = append(calls,
			RPCTask{
				inputFile,
				ip,
				*client.Go(
					"MapleJuiceRPCServer.RunMapleTask",
					MapleJuiceTask{
						InputFileName: inputFile,
						ExecFileName:  execFileName,
						OutputPrefix:  outputPrefix,
					},
					&mapleResults[cnt],
					nil),
			})
		cnt++
	}

	// Wait for all replies done
	for _, call := range calls {
		replyCall := <-call.call.Done
		if replyCall.Error != nil {
			log.Println("Some mapleTasks failed. Rescheduling is needed!", replyCall.Error)
			unfinishedTasks = append(unfinishedTasks, call.fileName)
			failedIP = append(failedIP, call.ip)
			continue
		}
	}

	// Reschedule unfinished mapleTasks
	mapleTasks = map[string]string{}
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
		mapleTasks[unfinishedTasks[i]] = node.(file_service.FileTableEntry).ServerIP
	}
	var newCalls []rpc.Call
	newResults := make([]string, len(unfinishedTasks))
	cnt = 0
	for inputFile, ip := range mapleTasks {
		client, err := rpc.Dial("tcp", ip+":"+port)
		if err != nil {
			log.Fatal(err)
		}

		newCalls = append(
			newCalls,
			*client.Go(
				"MapleJuiceRPCServer.RunMapleTask",
				MapleJuiceTask{
					InputFileName: inputFile,
					ExecFileName:  execFileName,
					OutputPrefix:  outputPrefix,
				},
				&newResults[cnt],
				nil))
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
	logger.PrintInfo("Start scheduling maple task...")

	start := time.Now().UnixNano() / int64(time.Millisecond)

	execFileName := cmd[1]
	executableFilePath := path.Join(mjServer.config.ExecDir, execFileName)
	taskNum, _ := strconv.Atoi(cmd[2])
	filenamePrefix := cmd[3]
	output := cmd[4]

	fmt.Println("Start searching for maple result files")
	// Find intermediate files
	files := mjServer.fileServer.FileTable.ListFilesByPrefix(filenamePrefix)
	fmt.Println("Done searching")

	fmt.Println("Start scheduling")
	// Schedule tasks (in turn)
	mjServer.fileServer.RemotePut(executableFilePath, execFileName)
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

			calls = append(calls,
				RPCTask{inputFile, ip,
					*client.Go("MapleJuiceRPCServer.RunJuiceTask",
						MapleJuiceTask{
							InputFileName: inputFile,
							ExecFileName:  execFileName,
						}, &juiceResults[cnt], nil)})
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
			newCalls = append(
				newCalls,
				*client.Go(
					"MapleJuiceRPCServer.RunJuiceTask",
					MapleJuiceTask{
						InputFileName: inputFile,
						ExecFileName:  execFileName,
					},
					&newResults[cnt], nil))
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
	mjServer.fileServer.RemoteAppend(content, output)
	fmt.Println("Done sorting")

	// RemoteDelete intermediate files
	if len(cmd) == 6 && cmd[5] == "1" {
		for _, file := range files {
			mjServer.fileServer.RemoteDelete(file)
		}
	}

	end := time.Now().UnixNano() / int64(time.Millisecond)
	log.Println("Juice elapsed", end - start, "milliseconds.")
}