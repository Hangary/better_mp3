package main

import (
	"better_mp3/app/command"
	"better_mp3/app/config"
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
	memberService    *member_service.MemberServer
	fileService      *file_service.FileServer
	maplejuiceServer *maple_juice_service.MapleJuiceServer
)

func HandleCommand() {
	inputReader := bufio.NewReader(os.Stdin)
	for {
		userInput, _ := inputReader.ReadString('\n')
		userInputs := strings.Split(strings.TrimSpace(userInput), " ")
		userCommand := command.Command{
			Method: userInputs[0],
			Params: userInputs[1:],
		}

		switch userCommand.Method {

		// member related commands
		case command.Join:
			memberService.HandleJoin(userCommand)
		case command.Display:
			memberService.HandleDisplay(userCommand)
		case command.Switch:
			memberService.HandleSwitch(userCommand)
		case command.Leave:
			memberService.HandleLeave(userCommand)

		// file related commands
		case "put":
			if len(userInputs) == 3 {
				start := time.Now()
				fileService.TemptPut(userInputs[1], userInputs[2])
				fmt.Println(" time to put file is", time.Since(start))
			}
		case "get":
			if len(userInputs) == 3 {
				start := time.Now()
				fileService.RemoteGet(userInputs[1], userInputs[2])
				fmt.Println(" time to get file is", time.Since(start))
			}
		case "delete":
			if len(userInputs) == 2 {
				start := time.Now()
				fileService.RemoteDelete(userInputs[1])
				fmt.Println(" time to delete file is", time.Since(start))
			}
		case "store":
			fileService.FileTable.ListMyFiles()
		case "ls":
			fmt.Println(fileService.FileTable.ListLocations(userInputs[1]))
		case "all":
			fileService.FileTable.ListAllFiles()

		// maple juice relate functions
		case "maple":
			if len(userInputs) == 5 {
				maplejuiceServer.ScheduleMapleTask(userInputs)
			}
		case "juice":
			if len(userInputs) == 5 || len(userInputs) == 6 {
				maplejuiceServer.ScheduleJuiceTask(userInputs)
			}

		default:
			logger.PrintWarning("Invalid userInputs.")
		}

	}
}

func main() {
	logger.PrintInfo("Loading config...")
	config.LoadConfig("./conf.yaml")

	logger.PrintInfo("Starting member service...")
	memberService = member_service.NewMemberServer()
	memberService.Run()

	logger.PrintInfo("Starting sdfs file service...")
	fileService = file_service.NewFileServer(memberService)
	fileService.Run()

	logger.PrintInfo("Starting maple juice service...")
	maplejuiceServer = maple_juice_service.NewMapleJuiceServer(fileService)
	maplejuiceServer.Run()

	logger.PrintInfo("Setup complete! You can input command now.")
	HandleCommand()
}
