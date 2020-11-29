package main

import (
	"better_mp3/app/file_service"
	"better_mp3/app/maple_juice_service"
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"
)

var mainChannel = make(chan string)

func HandleCommand(s *file_service.FileServer, mj *maple_juice_service.MapleJuiceServer) {
	s.MemberInfo.Run()
	for {
		buf := bufio.NewReader(os.Stdin)
		sentence, err := buf.ReadBytes('\n')
		if err != nil {
			fmt.Println(err)
		} else {
			cmd := strings.Split(string(bytes.Trim([]byte(sentence), "\n")), " ")
			fmt.Println("command: " + cmd[0])

			switch cmd[0] {

			// member related commands
			case "member":
				fmt.Println(s.MemberInfo.MemberList.Members)
			case "leave":
				s.MemberInfo.Leave()
			case "hash":
				fmt.Println(file_service.MyHash)
			case "ip":
				fmt.Println(s.MemberInfo.Ip)
			case "id":
				fmt.Println(s.MemberInfo.Id)

			// file related commands
			case "put":
				if len(cmd) == 3 {
					start := time.Now()
					s.TemptPut(cmd[1], cmd[2])
					fmt.Println(" time to put file is", time.Since(start))
				}
			case "get":
				if len(cmd) == 3 {
					start := time.Now()
					s.Get(cmd[1], cmd[2])
					fmt.Println(" time to get file is", time.Since(start))
				}
			case "delete":
				if len(cmd) == 2 {
					start := time.Now()
					s.Delete(cmd[1])
					fmt.Println(" time to delete file is", time.Since(start))
				}
			case "store":
				s.FileTable.ListMyFiles()
			case "ls":
				fmt.Println(s.FileTable.ListLocations(cmd[1]))
			case "all":
				s.FileTable.ListAllFiles()

			// maple juice relate functions
			case "maple":
				if len(cmd) == 5 {
					go mj.ScheduleMapleTask(cmd)
				}
			case "juice":
				if len(cmd) == 5 || len(cmd) == 6 {
					go mj.ScheduleJuiceTask(cmd)
				}
			}
		}
	}
}

func main() {
	fileServer := file_service.NewFileServer()
	go file_service.RunRPCServer(&fileServer)
	mjServer := maple_juice_service.NewMapleJuiceServer(&fileServer)
	go maple_juice_service.RunMapleJuiceRPCServer(&mjServer)
	HandleCommand(&fileServer, &mjServer)
	<-mainChannel
}
