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

func (r FileRPCServer) LocalDel(filename string, success *bool) error {
	return r.fileServer.LocalDel(filename, success)
}

func (r FileRPCServer) LocalGet(filename string, content *[]byte) error {
	return r.fileServer.LocalGet(filename, content)
}

func (r FileRPCServer) LocalAppend(args map[string]string, success *bool) error {
	return r.fileServer.LocalAppend(args, success)
}

func (r FileRPCServer) LocalPut(args map[string]string, success *bool) error {
	return r.fileServer.LocalPut(args, success)
}

func (r FileRPCServer) LocalRep(filename string, success *bool) error {
	return r.fileServer.LocalRep(filename, success)
}

func (r FileRPCServer) PutRepEntry(args map[uint32][]string, success *bool) error {
	return r.fileServer.FileTable.PutRepEntry(args, success)
}

func (r FileRPCServer) DelEntry(sdfs string, success *bool) error {
	return r.fileServer.FileTable.DeleteEntry(sdfs, success)
}

func (r FileRPCServer) PutEntry(sdfs string, success *bool) error {
	return r.fileServer.FileTable.PutEntry(sdfs, success)
}