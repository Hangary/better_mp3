package file_service

import (
	"log"
	"net"
	"net/rpc"
)

type FileRPCServer struct {
	fileServer *FileServer
}

func RunRPCServer(fileServer *FileServer) {
	server := FileRPCServer{
		fileServer: fileServer,
	}
	err := rpc.Register(server)
	if err != nil {
		log.Fatal("Failed to register RPC instance")
	}
	listener, err := net.Listen("tcp", ":"+fileServer.config.Port)
	for {
		rpc.Accept(listener)
	}
}

func (r FileRPCServer) LocalDelete(filename string, success *bool) error {
	return r.fileServer.LocalDelete(filename, success)
}

func (r FileRPCServer) LocalGet(filename string, content *[]byte) error {
	return r.fileServer.LocalGet(filename, content)
}

func (r FileRPCServer) LocalAppend(task FileTask, success *bool) error {
	return r.fileServer.LocalAppend(task, success)
}

func (r FileRPCServer) LocalPut(task FileTask, success *bool) error {
	return r.fileServer.LocalPut(task, success)
}

func (r FileRPCServer) LocalReplicate(filename string, success *bool) error {
	return r.fileServer.LocalReplicate(filename, success)
}

func (r FileRPCServer) PutRepEntry(args map[uint32][]string, success *bool) error {
	return r.fileServer.FileTable.PutRepEntry(args, success)
}

func (r FileRPCServer) DeleteEntry(sdfs string, success *bool) error {
	return r.fileServer.FileTable.DeleteEntry(sdfs, success)
}

func (r FileRPCServer) PutEntry(sdfs string, success *bool) error {
	return r.fileServer.FileTable.PutEntry(sdfs, success)
}