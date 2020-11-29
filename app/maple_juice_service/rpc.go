package maple_juice_service

import (
	"log"
	"net"
	"net/rpc"
)

type RPCTask struct {
	fileName string
	ip       string
	call     rpc.Call
}

type MapleJuiceRPCServer struct {
	mjServer *MapleJuiceServer
}

func RunMapleJuiceRPCServer(mjServer *MapleJuiceServer) {
	server := MapleJuiceRPCServer{
		mjServer: mjServer,
	}
	err := rpc.Register(server)
	if err != nil {
		log.Fatal("Failed to register RPC instance")
	}
	listener, err := net.Listen("tcp", ":"+server.mjServer.config.Port)
	for {
		rpc.Accept(listener)
	}
}

func (s MapleJuiceRPCServer) RunMapleTask(task MapleJuiceTask, mapleResult *string) error {
	return s.mjServer.RunMapleTask(task, mapleResult)
}

func (s MapleJuiceRPCServer) RunJuiceTask(task MapleJuiceTask, juiceResult *string) error {
	return s.mjServer.RunJuiceTask(task, juiceResult)
}