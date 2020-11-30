package maple_juice_service

import (
	"better_mp3/app/logger"
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

// execute maple_exe and get result file
func execute(execFileName string, inputFileName string, outputFileName string) error {
	cmd := "./" + execFileName + "<" + inputFileName + ">" + outputFileName
	_, err := exec.Command("/bin/sh", "-c", cmd).Output()
	return err
}

// return key-value pair
func splitMapleResultFile(resultFileName string) (kv map[string][]string, err error) {
	file, err := os.Open(resultFileName) // May need to updated to filePath
	if err != nil {
		fmt.Println("Can not open the maple_result file!")
		return nil, err
	}
	defer file.Close()

	kv = make(map[string][]string)
	result := bufio.NewScanner(file)
	for result.Scan() {
		currLine := result.Text()
		currLine = strings.TrimSpace(currLine)
		currLineSplit := strings.Split(currLine, " ")
		key := currLineSplit[0]

		kv[key] = append(kv[key], currLine)
	}
	return
}


func (mjServer *MapleJuiceServer) RunMapleTask(task MapleJuiceTask, mapleResult *string) error {
	fmt.Println("Start running Maple task")

	logger.PrintInfo("Getting executable file from SDFS...")
	mjServer.fileServer.RemoteGet(
		task.ExecFileName,
		path.Join(mjServer.config.TmpDir, task.ExecFileName))

	logger.PrintInfo("Getting input file clip from SDFS...")
	mjServer.fileServer.RemoteGet(
		task.InputFileName,
		path.Join(mjServer.config.TmpDir, task.InputFileName))

	logger.PrintInfo("Running maple executable...")
	err := execute(
		path.Join(mjServer.config.TmpDir, task.ExecFileName),
		path.Join(mjServer.config.TmpDir, task.InputFileName),
		path.Join(mjServer.config.TmpDir, task.OutputPrefix + "-" + "TMP"))
	if err != nil {
		logger.PrintError(err)
	}

	logger.PrintInfo("Splitting maple result...")
	kv, err := splitMapleResultFile(path.Join(mjServer.config.TmpDir, task.OutputPrefix + "-" + "TMP"))
	if err != nil {
		logger.PrintError(err)
		return err
	}

	logger.PrintInfo("Uploading maple result...")
	for key, value := range kv {
		mjServer.fileServer.RemoteAppend(strings.Join(value, "\n") + "\n", task.OutputPrefix + "_" + key)
	}

	logger.PrintInfo("Successfully finished maple task!")
	return nil
}

func (mjServer *MapleJuiceServer) RunJuiceTask(task MapleJuiceTask, juiceResult *string) error {
	fmt.Println("Start running Juice task...")

	logger.PrintInfo("Getting executable file from SDFS...")
	mjServer.fileServer.RemoteGet(
		task.ExecFileName,
		path.Join(mjServer.config.TmpDir, task.ExecFileName))

	logger.PrintInfo("Getting input file clip from SDFS...")
	mjServer.fileServer.RemoteGet(
		task.InputFileName,
		path.Join(mjServer.config.TmpDir, task.InputFileName))

	time.Sleep(time.Second)

	logger.PrintInfo("Running juice executable...")
	logger.PrintInfo("DEBUG:",
		path.Join(mjServer.config.TmpDir, task.ExecFileName),
		path.Join(mjServer.config.TmpDir, task.InputFileName))
	cmd := exec.Command(
		path.Join(mjServer.config.TmpDir, task.ExecFileName),
		path.Join(mjServer.config.TmpDir, task.InputFileName))
	ret, err := cmd.CombinedOutput()
	if err != nil {
		logger.PrintError(err)
		return err
	}
	logger.PrintInfo("DEBUG:", ret)

	// RemoteGet the resulting key-value pair
	logger.PrintInfo("Successfully finished juice task!")
	*juiceResult = string(ret)
	return nil
}
