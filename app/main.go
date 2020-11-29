package main

import (
	"better_mp3/app/file_service"
	"better_mp3/app/logger"
	"better_mp3/app/maple_juice_service"
	"better_mp3/app/member_service"
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"
)

var (
	memberService    member_service.MemberServer
	fileService      file_service.FileServer
	maplejuiceServer maple_juice_service.MapleJuiceServer
)

func HandleCommand(s *file_service.FileServer, mj *maple_juice_service.MapleJuiceServer) {
	inputReader := bufio.NewReader(os.Stdin)
	for {
		userInput, _ := inputReader.ReadString('\n')
		command := strings.Split(strings.TrimSpace(userInput), " ")

		switch command[0] {

		// member related commands
		case "member":
			memberService.PrintMemberList()
		case "leave":
			memberService.Leave()
		case "hash":
			fmt.Println(file_service.MyHash)
		case "ip":
			fmt.Println(s.MemberInfo.SelfIP)
		case "id":
			fmt.Println(s.MemberInfo.SelfID)

		// file related commands
		case "put":
			if len(command) == 3 {
				start := time.Now()
				s.TemptPut(command[1], command[2])
				fmt.Println(" time to put file is", time.Since(start))
			}
		case "get":
			if len(command) == 3 {
				start := time.Now()
				s.Get(command[1], command[2])
				fmt.Println(" time to get file is", time.Since(start))
			}
		case "delete":
			if len(command) == 2 {
				start := time.Now()
				s.Delete(command[1])
				fmt.Println(" time to delete file is", time.Since(start))
			}
		case "store":
			s.FileTable.ListMyFiles()
		case "ls":
			fmt.Println(s.FileTable.ListLocations(command[1]))
		case "all":
			s.FileTable.ListAllFiles()

		// maple juice relate functions
		case "maple":
			if len(command) == 5 {
				go mj.ScheduleMapleTask(command)
			}
		case "juice":
			if len(command) == 5 || len(command) == 6 {
				go mj.ScheduleJuiceTask(command)
			}

		default:
			logger.PrintWarning("Invalid command.")
		}

	}
}

func main() {
	logger.PrintInfo("Starting member service...")
	memberService = member_service.NewMemberServer()
	memberService.Run()

	logger.PrintInfo("Starting sdfs file service...")
	fileService = file_service.NewFileServer(memberService)
	fileService.Run()

	logger.PrintInfo("Starting maple juice service...")
	maplejuiceServer = maple_juice_service.NewMapleJuiceServer(&fileService)
	maplejuiceServer.Run()

	logger.PrintInfo("Setup complete! You can input command now.")
	HandleCommand(&fileService, &maplejuiceServer)
}
