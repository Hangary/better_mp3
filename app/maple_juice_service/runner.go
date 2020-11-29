package maple_juice_service

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"
)

func (mjServer *MapleJuiceServer) RunMapleTask(args map[string]string, mapleResult *string) error {
	fmt.Println("Start running Maple task")

	inputFile := args["input"]
	executable := args["executable"]
	outputPrefix := args["output_prefix"]

	fmt.Println("Getting executable from SDFS")
	// RemoteGet executable from DFS
	mjServer.fileServer.RemoteGet(executable, path.Join(mjServer.config.AppPath, executable))

	fmt.Println("Getting input from SDFS")
	// RemoteGet input file from DFS
	mjServer.fileServer.RemoteGet(inputFile, path.Join(mjServer.config.AppPath, inputFile))

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
		cmd := exec.Command(path.Join(mjServer.config.AppPath, executable), strings.Join(content, "\n"))
		ret, err := cmd.CombinedOutput()
		if err != nil {
			log.Println("Application error: ", err)
			return err
		}

		//fmt.Println("RemoteGet maple results")
		// RemoteGet resulting key-value pairs
		var results []string
		err = json.Unmarshal(ret, &results)
		if err != nil {
			log.Println(err)
		}
		*mapleResult = strings.Join(results, "\n")

		//fmt.Println("Write maple results")
		// RemoteAppend intermediate result to DFS
		reg, err := regexp.Compile("[^a-zA-Z0-9]+")
		if err != nil {
			log.Fatalln(err)
		}
		for _, pair := range results {
			tmp := strings.Split(pair, ",")
			key := reg.ReplaceAllString(tmp[0], "")
			mjServer.fileServer.RemoteAppend(pair + "\n", outputPrefix+"_"+key)
			//time.Sleep(time.Millisecond * 200)
		}
	}
	if err := f.Close(); err != nil {
		log.Fatalln(err)
	}

	return nil
}

func (mjServer *MapleJuiceServer) RunJuiceTask(args map[string]string, juiceResult *string) error {
	fmt.Println("Start running Juice task")

	inputFile := args["input"]
	executable := args["executable"]

	//fmt.Println("RemoteGet executable from DFS")
	// RemoteGet executable executable from DFS
	mjServer.fileServer.RemoteGet(executable, path.Join(mjServer.config.AppPath, executable))

	//fmt.Println("RemoteGet input from DFS")
	// RemoteGet input file from DFS
	mjServer.fileServer.RemoteGet(inputFile, path.Join(mjServer.config.AppPath, inputFile))

	time.Sleep(time.Second)

	//fmt.Println("Call juice function")
	// Call juice function
	var ret []byte
	var err error
	for {
		cmd := exec.Command(path.Join(mjServer.config.AppPath, executable), path.Join(mjServer.config.AppPath, inputFile))
		ret, err = cmd.CombinedOutput()
		if err == nil {
			break
		}
		log.Println(err)
	}

	//fmt.Println("RemoteGet juice results")
	// RemoteGet the resulting key-value pair
	*juiceResult = string(ret)
	return nil
}
