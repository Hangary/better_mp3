package file_service

import (
	"better_mp3/app/config"
	"better_mp3/app/logger"
	"better_mp3/app/member_service"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

var promptChannel = make(chan string)
var MyHash uint32

type FileServer struct {
	ms        *member_service.MemberServer
	FileTable FileTable
	config    config.FileServiceConfig
}

type FileTask struct {
	FileName string
	Content []byte
}

func NewFileServer(memberService *member_service.MemberServer) *FileServer {
	var fs FileServer
	fs.config = config.GetFileServiceConfig()
	fs.ms = memberService
	fs.FileTable = NewFileTable(&fs)
	go fs.RunDaemon()
	return &fs
}

func (fs *FileServer) Run() {
	go RunRPCServer(fs)
	logger.PrintInfo(
		"File Service is now running on port " + fs.config.Port,
		"\n\tSDFS file path: ", fs.config.Path)
}

func (fs *FileServer) LocalReplicate(filename string, success *bool) error {
	var content []byte
	locations := fs.FileTable.ListLocations(filename)
	if len(locations) == 0 {
		return errors.New("no replica available")
	} else {
		for _, ip := range locations {
			var buffer []byte
			if ip == fs.ms.SelfIP {
				err := fs.LocalGet(filename, &buffer)
				if err != nil {
					continue
				}
			} else {
				client, err := rpc.Dial("tcp", ip+":"+fs.config.Port)
				if err != nil {
					continue
				}
				err = client.Call("FileRPCServer.LocalGet", filename, &buffer)
				if err != nil {
					continue
				}
			}
			content = buffer
		}
	}

	if content != nil {
		err := fs.LocalPut(FileTask {
			FileName: filename,
			Content: content,
		}, nil)
		return err
	} else {
		return nil
	}
}

func (fs *FileServer) LocalPut(task FileTask, success *bool) error {
	err := ioutil.WriteFile(fs.config.Path + task.FileName, task.Content, os.ModePerm)
	return err
}

func (fs *FileServer) LocalAppend(task FileTask, success *bool) error {
	f, err := os.OpenFile(fs.config.Path + task.FileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(task.Content); err != nil {
		return err
	}
	err = f.Close()
	return err
}

// local: local file name
// remote: remote file name
func (fs *FileServer) RemotePut(local string, remote string) {
	target_ips := fs.FileTable.search(remote)
	//fmt.Println(target_ips)
	for _, ip := range target_ips {
		content, err := ioutil.ReadFile(local)
		if err != nil {
			fmt.Println("Local file", local, "doesn't exist!")
			return
		} else {
			client, err := rpc.Dial("tcp", ip+":"+fs.config.Port)
			if err != nil {
				log.Println(err)
				continue
			}
			var success bool
			err = client.Call("FileRPCServer.LocalPut",
				FileTask {
				FileName: remote,
				Content:  content,
				}, &success)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}
	err := fs.FileTable.PutEntry(remote, nil)
	if err != nil {
		log.Println(err)
	}

	for _, memberIP := range fs.ms.GetAliveMemberIPList() {
		client, err := rpc.Dial("tcp", memberIP+":"+fs.config.Port)
		if err != nil {
			log.Println(err)
			continue
		}
		err = client.Call("FileRPCServer.PutEntry", remote, nil)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

func (fs *FileServer) LocalGet(filename string, content *[]byte) error {
	var err error
	*content, err = ioutil.ReadFile(fs.config.Path + filename)
	return err
}

func (fs *FileServer) RemoteGet(sdfs string, local string) {
	locations := fs.FileTable.ListLocations(sdfs)
	if len(locations) == 0 {
		fmt.Println("The file is not available!")
	} else {
		for _, ip := range locations {
			var buffer []byte
			if ip == fs.ms.SelfIP {
				err := fs.LocalGet(sdfs, &buffer)
				if err != nil {
					continue
				}
			} else {
				client, err := rpc.Dial("tcp", ip+":"+fs.config.Port)
				if err != nil {
					continue
				}
				err = client.Call("FileRPCServer.LocalGet", sdfs, &buffer)
				if err != nil {
					continue
				}
			}
			err := ioutil.WriteFile(local, buffer, os.ModePerm)
			if err != nil {
				continue
			}
			break
		}
	}
}

func (fs *FileServer) LocalDelete(filename string, success *bool) error {
	err := os.Remove(fs.config.Path + filename)
	return err
}

func (fs *FileServer) RemoteDelete(sdfs string) {
	locations := fs.FileTable.ListLocations(sdfs)
	if len(locations) == 0 {
		fmt.Println("The file is not available!")
	} else {
		//fmt.Println(locations)
		var success bool
		for _, ip := range locations {
			if ip == fs.ms.SelfIP {
				err := fs.LocalDelete(sdfs, &success)
				if err != nil {
					log.Println(err)
				}
			} else {
				client, err := rpc.Dial("tcp", ip+":"+fs.config.Port)
				if err != nil {
					log.Println(err)
					continue
				}
				err = client.Call("FileRPCServer.LocalDelete", sdfs, &success)
				if err != nil {
					log.Println(err)
					continue
				}
			}
		}
		err := fs.FileTable.DeleteEntry(sdfs, &success)
		if err != nil {
			log.Println(err)
		}
		for _, memberIP := range fs.ms.GetAliveMemberIPList() {
			client, err := rpc.Dial("tcp", memberIP+":"+fs.config.Port)
			if err != nil {
				log.Println(err)
				continue
			}
			err = client.Call("FileRPCServer.DeleteEntry", sdfs, &success)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}
}

func (fs *FileServer) RemoteAppend(content []byte, remoteFileName string) {
	target_ips := fs.FileTable.search(remoteFileName)
	//fmt.Println(target_ips)
	for _, ip := range target_ips {
		client, err := rpc.Dial("tcp", ip+":"+fs.config.Port)
		if err != nil {
			log.Println(err)
			continue
		}
		var success bool
		err = client.Call(
			"FileRPCServer.LocalAppend",
			FileTask {
				FileName: remoteFileName,
				Content:  content,
			}, &success)
		if err != nil {
			log.Println(err)
			continue
		}
	}
	var success bool
	err := fs.FileTable.PutEntry(remoteFileName, &success)
	if err != nil {
		log.Println(err)
	}
	for _, memberIP := range fs.ms.GetAliveMemberIPList() {
		client, err := rpc.Dial("tcp", memberIP+ ":" + fs.config.Port)
		if err != nil {
			logger.PrintError(err)
			continue
		}
		err = client.Call("FileRPCServer.PutEntry", remoteFileName, &success)
		if err != nil {
			logger.PrintError(err)
			continue
		}
	}
}

