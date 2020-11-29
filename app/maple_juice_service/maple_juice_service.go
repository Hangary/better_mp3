package maple_juice_service

import (
	"better_mp3/app/config"
	"better_mp3/app/file_service"
	"better_mp3/app/logger"
)

type MapleJuiceServer struct {
	config     config.MapleJuiceServiceConfig
	fileServer *file_service.FileServer
}

func NewMapleJuiceServer(fileServer *file_service.FileServer) *MapleJuiceServer {
	var f MapleJuiceServer
	f.config = config.GetMapleJuiceServiceConfig()
	f.fileServer = fileServer
	return &f
}

func (mjServer *MapleJuiceServer) Run() {
	go RunMapleJuiceRPCServer(mjServer)

	logger.PrintInfo(
		"MapleJuice Service is now running on port " + mjServer.config.Port,
		"\n")
}
