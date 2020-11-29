package member_service

import (
	"better_mp3/app/logger"
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ADDED = iota
	JOINED
	SUSPECTED
	LEAVING
	LEFT
)
type Status int

func FindLocalhostIp() string {
	addrs, _ := net.InterfaceAddrs()
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			return ipnet.IP.String()
		}
	}
	return ""
}

var mux sync.Mutex

type MemberServer struct {
	heartbeatCounter int
	timestamp        int64
	status           int
	config           MemberServiceConfig
	failedNodesList  []string

	SelfIP string
	SelfID string

	// leave channel
	LeaveChannel chan string
	// these two channels are used by upper level service
	LeftNodesChan  chan []string
	JoinedNodeChan chan string

	Members map[string]map[string]int
}

func getIpFromId(id string) string {
	return strings.Split(id, "_")[0]
}

func (ms *MemberServer) UpdateMembership(message map[string]map[string]int) {
	for id, msg := range message {
		if getIpFromId(id) == ms.config.Introducer {
			_, ok := ms.Members[ms.config.Introducer]
			if ok {
				delete(ms.Members, ms.config.Introducer)
			}
		}
		if msg["status"] == LEFT {
			_, ok := ms.Members[id]
			if ok && ms.Members[id]["status"] != LEFT {
				ms.Members[id] = map[string]int{
					"heartbeat": msg["heartbeat"],
					"timestamp": int(time.Now().UnixNano() / int64(time.Millisecond)),
					"status":    LEFT,
				}
				log.Println("[member left]", id, ":", ms.Members[id])
			}
		} else if msg["status"] == JOINED {
			_, ok := ms.Members[id]
			if !ok || msg["heartbeat"] > ms.Members[id]["heartbeat"] {
				ms.Members[id] = map[string]int{
					"heartbeat": msg["heartbeat"],
					"timestamp": int(time.Now().UnixNano() / int64(time.Millisecond)),
					"status":    JOINED,
				}
			}
			if !ok {
				ms.JoinedNodeChan <- strings.Split(id, "_")[0]
				log.Println("[member joined]", id, ":", ms.Members[id])
			}
		} else {
			log.Println("ERROR unknown status")
		}
	}
}

func (ms *MemberServer) CheckFailure() {
	for id, info := range ms.Members {
		switch info["status"] {
		case JOINED:
			if int(time.Now().UnixNano()/int64(time.Millisecond))-info["timestamp"] > ms.config.SuspectTime {
				info["status"] = SUSPECTED
				log.Println("[suspected]", id, ":", ms.Members[id])
			}
		case SUSPECTED:
			if int(time.Now().UnixNano()/int64(time.Millisecond))-info["timestamp"] > ms.config.FailTime {
				delete(ms.Members, id)
				ms.failedNodesList = append(ms.failedNodesList, strings.Split(id, "_")[0])
				if len(ms.failedNodesList) == 1 {
					time.AfterFunc(4*time.Second, func() {
						ms.LeftNodesChan <- ms.failedNodesList
						ms.failedNodesList = ms.failedNodesList[:0]
					})
				}
				log.Println("[failed]", id, ":", ms.Members[id])
			}
		case LEFT:
			if int(time.Now().UnixNano()/int64(time.Millisecond))-info["timestamp"] > ms.config.RemoveTime {
				delete(ms.Members, id)
				ms.LeftNodesChan <- ms.failedNodesList
				log.Println("[removed]", id, ":", ms.Members[id])
			}
		}
	}
}

func (ms *MemberServer) FindGossipDest(status int) string {
	var candidates []string
	for id, info := range ms.Members {
		if info["status"] == JOINED || info["status"] == status {
			candidates = append(candidates, id)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s).Intn(len(candidates))
	return getIpFromId(candidates[r])
}

func NewMemberServer() MemberServer {
	var ms MemberServer
	ms.config = GetMemberServiceConfig()
	ms.SelfIP = FindLocalhostIp()
	if ms.SelfIP == "" {
		log.Fatal("ERROR get localhost IP")
	}
	t := time.Now().UnixNano() / int64(time.Millisecond)
	ms.SelfID = ms.SelfIP + "_" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	ms.heartbeatCounter = 1
	ms.timestamp = t
	ms.status = JOINED
	ms.Members = map[string]map[string]int{}
	ms.JoinedNodeChan = make(chan string, 10)
	ms.LeftNodesChan = make(chan []string, 10)
	if ms.SelfIP != ms.config.Introducer {
		id := ms.config.Introducer
		ms.Members[id] = map[string]int{
			"heartbeat": 0,
			"timestamp": 0,
			"status":    ADDED,
		}
		logger.PrintInfo("[introducer added]", id, ":", ms.Members[id])
	}
	return ms
}

func (ms *MemberServer) Heartbeat(leaving bool) {
	ms.heartbeatCounter += 1
	ms.timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	if leaving {
		ms.status = LEFT
	}
}

func (ms *MemberServer) MakeMessage(dest string) map[string]map[string]int {
	message := map[string]map[string]int{}
	for id, info := range ms.Members {
		if (info["status"] == JOINED || info["status"] == LEFT) && strings.Split(id, "_")[0] != dest {
			message[id] = map[string]int{
				"heartbeat": info["heartbeat"],
				"status":    info["status"],
			}
		}
	}
	message[ms.SelfID] = map[string]int{
		"heartbeat": ms.heartbeatCounter,
		"status":    ms.status,
	}
	return message
}

func (ms *MemberServer) Gossip() {
	logger.PrintInfo("Start to gossip")
	for {
		mux.Lock()
		ms.CheckFailure()
		if ms.status == LEAVING {
			dest := ms.FindGossipDest(JOINED)
			if dest != "" {
				ms.Heartbeat(true)
				message := ms.MakeMessage(dest)
				conn, err := net.Dial("tcp", dest+":"+ms.config.Port)
				if err == nil {
					marshaled, err := json.Marshal(message)
					if err == nil {
						_, _ = conn.Write([]byte(string(marshaled) + "\n"))
					}
					_ = conn.Close()
				}
			}
			break
		} else if ms.status == JOINED {
			dest := ms.FindGossipDest(ADDED)
			if dest != "" {
				ms.Heartbeat(false)
				message := ms.MakeMessage(dest)
				conn, err := net.Dial("tcp", dest+":"+ms.config.Port)
				if err == nil {
					marshaled, err := json.Marshal(message)
					if err == nil {
						_, _ = conn.Write([]byte(string(marshaled) + "\n"))
					}
					_ = conn.Close()
				}
			}
		} else {
			log.Println("ERROR unknown status")
		}
		mux.Unlock()
		time.Sleep(ms.config.GossipInterval * time.Millisecond)
	}
	defer func() { ms.LeaveChannel <- "OK" }()
}

func (ms *MemberServer) ListenHeartbeat() {
	logger.PrintInfo("Start to listen")
	ln, err := net.Listen("tcp", ":"+ms.config.Port)
	if err != nil {
		log.Fatal(err)
	}
	for ms.status == JOINED {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		message, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}
		if message != "" {
			var struct_msg map[string]map[string]int
			err := json.Unmarshal([]byte(message), &struct_msg)
			if err != nil {
				log.Fatal(err)
			}
			mux.Lock()
			ms.UpdateMembership(struct_msg)
			mux.Unlock()
		}
		_ = conn.Close()
	}
}

func (ms *MemberServer) PrintMemberList() {
	fmt.Println(ms.Members)
}

func (ms *MemberServer) Leave() {
	ms.status = LEAVING
	logger.PrintInfo("Leaving...")
	<-ms.LeaveChannel
	logger.PrintInfo("Left successfully")
}

func (ms *MemberServer) Run() {
	go ms.Gossip()
	go ms.ListenHeartbeat()
	logger.PrintInfo(
		"Member Service is now running on port " + ms.config.Port,
		"\n\tIntroducer: ", ms.config.Introducer,
		"\n\tMember Self ID:", ms.SelfID)
}
