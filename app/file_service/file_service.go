package file_service

import (
	"better_mp3/app/member_service"
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

var promptChannel = make(chan string)
var MyHash uint32

type FileServer struct {
	ip         string
	MemberInfo member_service.MemberInfo
	FileTable  FileTable
	config     ServerConf
}

func NewFileServer() FileServer {
	var f FileServer
	f.config = NewServerConf()
	f.ip = member_service.FindLocalhostIp()
	if f.ip == "" {
		log.Fatal("ERROR get localhost IP")
	}
	f.FileTable = NewFileTable(&f)
	f.MemberInfo = member_service.NewMemberService()
	return f
}

func (s *FileServer) LocalRep(filename string, success *bool) error {
	var content string
	locations := s.FileTable.ListLocations(filename)
	if len(locations) == 0 {
		return errors.New("no replica available")
	} else {
		for _, ip := range locations {
			var buffer []byte
			if ip == s.ip {
				err := s.LocalGet(filename, &buffer)
				if err != nil {
					continue
				}
			} else {
				client, err := rpc.Dial("tcp", ip+":"+s.config.Port)
				if err != nil {
					continue
				}
				err = client.Call("FileRPCServer.LocalGet", filename, &buffer)
				if err != nil {
					continue
				}
			}
			content = string(buffer)
		}
	}
	err := s.LocalPut(map[string]string{"filename": filename, "content": content}, success)
	return err
}


func (s *FileServer) LocalPut(args map[string]string, success *bool) error {
	err := ioutil.WriteFile(s.config.Path+args["filename"], []byte(args["content"]), os.ModePerm)
	return err
}


func (s *FileServer) LocalAppend(args map[string]string, success *bool) error {
	f, err := os.OpenFile(s.config.Path+args["filename"], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(args["content"])); err != nil {
		return err
	}
	err = f.Close()
	return err
}

func (s *FileServer) confirm(local string, remote string) {
	buf := bufio.NewReader(os.Stdin)
	go func() {
		time.Sleep(time.Second * 30)
		promptChannel <- "ok"
	}()
	for {
		select {
		case <-promptChannel:
			fmt.Println("Timeout")
			return
		default:
			sentence, err := buf.ReadBytes('\n')
			cmd := strings.Split(string(bytes.Trim([]byte(sentence), "\n")), " ")
			if err == nil && len(cmd) == 1 {
				if cmd[0] == "y" || cmd[0] == "yes" {
					s.Put(local, remote)
				} else if cmd[0] == "n" || cmd[0] == "no" {
					return
				}
			}
		}
	}
}

func (s *FileServer) TemptPut(local string, remote string) {
	_, ok := s.FileTable.latest[remote]
	if ok && time.Now().UnixNano()-s.FileTable.latest[remote] < int64(time.Minute) {
		fmt.Println("Confirm update? (y/n)")
		s.confirm(local, remote)
	} else {
		s.Put(local, remote)
	}
}

// local: local file name
// remote: remote file name
func (s *FileServer) Put(local string, remote string) {
	target_ips := s.FileTable.search(remote)
	//fmt.Println(target_ips)
	for _, ip := range target_ips {
		content, err := ioutil.ReadFile(local)
		if err != nil {
			fmt.Println("Local file", local, "doesn't exist!")
			return
		} else {
			client, err := rpc.Dial("tcp", ip+":"+s.config.Port)
			if err != nil {
				log.Println(err)
				continue
			}
			var success bool
			err = client.Call("FileRPCServer.LocalPut", map[string]string{
				"filename": remote,
				"content":  string(content),
			}, &success)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}
	var success bool
	err := s.FileTable.PutEntry(remote, &success)
	if err != nil {
		log.Println(err)
	}
	for id, _ := range s.MemberInfo.MemberList.Members {
		client, err := rpc.Dial("tcp", strings.Split(id, "_")[0]+":"+s.config.Port)
		if err != nil {
			log.Println(err)
			continue
		}
		err = client.Call("FileRPCServer.PutEntry", remote, &success)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

func (s *FileServer) LocalGet(filename string, content *[]byte) error {
	var err error
	*content, err = ioutil.ReadFile(s.config.Path + filename)
	return err
}

func (s *FileServer) Get(sdfs string, local string) {
	locations := s.FileTable.ListLocations(sdfs)
	if len(locations) == 0 {
		fmt.Println("The file is not available!")
	} else {
		for _, ip := range locations {
			var buffer []byte
			if ip == s.ip {
				err := s.LocalGet(sdfs, &buffer)
				if err != nil {
					continue
				}
			} else {
				client, err := rpc.Dial("tcp", ip+":"+s.config.Port)
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

func (s *FileServer) LocalDel(filename string, success *bool) error {
	err := os.Remove(s.config.Path + filename)
	return err
}

func (s *FileServer) Delete(sdfs string) {
	locations := s.FileTable.ListLocations(sdfs)
	if len(locations) == 0 {
		fmt.Println("The file is not available!")
	} else {
		//fmt.Println(locations)
		var success bool
		for _, ip := range locations {
			if ip == s.ip {
				err := s.LocalDel(sdfs, &success)
				if err != nil {
					log.Println(err)
				}
			} else {
				client, err := rpc.Dial("tcp", ip+":"+s.config.Port)
				if err != nil {
					log.Println(err)
					continue
				}
				err = client.Call("FileRPCServer.LocalDel", sdfs, &success)
				if err != nil {
					log.Println(err)
					continue
				}
			}
		}
		err := s.FileTable.DeleteEntry(sdfs, &success)
		if err != nil {
			log.Println(err)
		}
		for id, _ := range s.MemberInfo.MemberList.Members {
			client, err := rpc.Dial("tcp", strings.Split(id, "_")[0]+":"+s.config.Port)
			if err != nil {
				log.Println(err)
				continue
			}
			err = client.Call("FileRPCServer.DelEntry", sdfs, &success)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}
}

func (s *FileServer) Append(content string, remote string) {
	target_ips := s.FileTable.search(remote)
	//fmt.Println(target_ips)
	for _, ip := range target_ips {
		client, err := rpc.Dial("tcp", ip+":"+s.config.Port)
		if err != nil {
			log.Println(err)
			continue
		}
		var success bool
		err = client.Call("FileRPCServer.LocalAppend", map[string]string{
			"filename": remote,
			"content":  content,
		}, &success)
		if err != nil {
			log.Println(err)
			continue
		}
	}
	var success bool
	err := s.FileTable.PutEntry(remote, &success)
	if err != nil {
		log.Println(err)
	}
	for id, _ := range s.MemberInfo.MemberList.Members {
		client, err := rpc.Dial("tcp", strings.Split(id, "_")[0]+":"+s.config.Port)
		if err != nil {
			log.Println(err)
			continue
		}
		err = client.Call("FileRPCServer.PutEntry", remote, &success)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

