package member_service

import (
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
	ADDED     = 0
	JOINED    = 1
	SUSPECTED = 2
	LEAVING   = 4
	LEFT      = 5
)

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

type MemberInfo struct {
	Ip               string
	Id               string
	heartbeatCounter int
	timestamp        int64
	status           int
	MemberList       MemberList
	config           DaemonConf
	// leave channel
	LeaveChannel 	chan string
}

type MemberList struct {
	memberInfo 		*MemberInfo
	Members         map[string]map[string]int
	failedNodesList []string
	// these two channels are used by upper level service
	leftNodesChan   chan []string
	joinedNodeChan chan string
}



func NewMemberList(d *MemberInfo) MemberList {
	var m MemberList
	m.memberInfo = d
	// todo: m.table = f
	m.joinedNodeChan = make(chan string, 10) // todo:
	m.leftNodesChan = make(chan []string, 10) // todo:
	m.Members = map[string]map[string]int{}
	return m
}

func (m *MemberList) UpdateMembership(message map[string]map[string]int) {
	for id, msg := range message {
		if strings.Split(id, "_")[0] == m.memberInfo.config.Introducer {
			_, ok := m.Members[m.memberInfo.config.Introducer]
			if ok {
				delete(m.Members, m.memberInfo.config.Introducer)
			}
		}
		if msg["status"] == LEFT {
			_, ok := m.Members[id]
			if ok && m.Members[id]["status"] != LEFT {
				m.Members[id] = map[string]int{
					"heartbeat": msg["heartbeat"],
					"timestamp": int(time.Now().UnixNano() / int64(time.Millisecond)),
					"status":    LEFT,
				}
				log.Println("[member left]", id, ":", m.Members[id])
			}
		} else if msg["status"] == JOINED {
			_, ok := m.Members[id]
			if !ok || msg["heartbeat"] > m.Members[id]["heartbeat"] {
				m.Members[id] = map[string]int{
					"heartbeat": msg["heartbeat"],
					"timestamp": int(time.Now().UnixNano() / int64(time.Millisecond)),
					"status":    JOINED,
				}
			}
			if !ok {
				// todo: m.table.AddEmptyEntry(strings.Split(id, "_")[0])
				m.joinedNodeChan <- strings.Split(id, "_")[0] //todo:
				log.Println("[member joined]", id, ":", m.Members[id])
			}
		} else {
			log.Println("ERROR unknown status")
		}
	}
}

func (m *MemberList) DetectFailure() {
	for id, info := range m.Members {
		if info["status"] == JOINED {
			if int(time.Now().UnixNano() / int64(time.Millisecond))-info["timestamp"] > m.memberInfo.config.SuspectTime {
				info["status"] = SUSPECTED
				log.Println("[suspected]", id, ":", m.Members[id])
			}
		} else if info["status"] == SUSPECTED {
			if int(time.Now().UnixNano() / int64(time.Millisecond))-info["timestamp"] > m.memberInfo.config.FailTime {
				delete(m.Members, id)
				m.failedNodesList = append(m.failedNodesList, strings.Split(id, "_")[0])
				if len(m.failedNodesList) == 1 {
					time.AfterFunc(4 * time.Second, func() {
						// todo: m.table.RemoveFromTable(m.failedNodes)
						m.leftNodesChan <- m.failedNodesList // todo: fixing

						m.failedNodesList = m.failedNodesList[:0]
					})
				}
				log.Println("[failed]", id, ":", m.Members[id])
			}
		} else if info["status"] == LEFT {
			if int(time.Now().UnixNano() / int64(time.Millisecond))-info["timestamp"] > m.memberInfo.config.RemoveTime {
				delete(m.Members, id)
				// todo: m.table.RemoveFromTable([]string{strings.Split(id, "_")[0]})
				m.leftNodesChan <- m.failedNodesList // todo: fixing
				log.Println("[removed]", id, ":", m.Members[id])
			}
		}
	}
}

func (m *MemberList) FindGossipDest(status int) string {
	var candidates []string
	for id, info := range m.Members {
		if info["status"] == JOINED || info["status"] == status {
			candidates = append(candidates, id)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s).Intn(len(candidates))
	return strings.Split(candidates[r], "_")[0]
}



func NewMemberService() MemberInfo {
	var d MemberInfo
	d.config = NewDaemonConf()
	d.Ip = FindLocalhostIp()
	if d.Ip == "" {
		log.Fatal("ERROR get localhost IP")
	}
	t := time.Now().UnixNano() / int64(time.Millisecond)
	d.Id = d.Ip + "_" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	d.heartbeatCounter = 1
	d.timestamp = t
	d.status = JOINED
	// todo: d.MemberList = NewMemberList(&d, filetableptr)
	d.MemberList = NewMemberList(&d) //todo:
	if d.Ip != d.config.Introducer {
		id := d.MemberList.memberInfo.config.Introducer
		d.MemberList.Members[id] = map[string]int{
			"heartbeat": 0,
			"timestamp": 0,
			"status":    ADDED,
		}
		log.Println("[introducer added]", id, ":", d.MemberList.Members[id])
	}
	return d
}

func (d *MemberInfo) Heartbeat(leaving bool) {
	d.heartbeatCounter += 1
	d.timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	if leaving {
		d.status = LEFT
	}
}

func (d *MemberInfo) MakeMessage(dest string) map[string]map[string]int {
	message := map[string]map[string]int{}
	for id, info := range d.MemberList.Members {
		if (info["status"] == JOINED || info["status"] == LEFT) && strings.Split(id, "_")[0] != dest {
			message[id] = map[string]int{
				"heartbeat": info["heartbeat"],
				"status":    info["status"],
			}
		}
	}
	message[d.Id] = map[string]int{
		"heartbeat": d.heartbeatCounter,
		"status":    d.status,
	}
	return message
}

func (d *MemberInfo) Gossip() {
	fmt.Println("Start to gossip")
	for {
		mux.Lock()
		d.MemberList.DetectFailure()
		if d.status == LEAVING {
			dest := d.MemberList.FindGossipDest(JOINED)
			if dest != "" {
				d.Heartbeat(true)
				message := d.MakeMessage(dest)
				conn, err := net.Dial("tcp", dest+":"+d.config.Port)
				if err == nil {
					marshaled, err := json.Marshal(message)
					if err == nil {
						_, _ = conn.Write([]byte(string(marshaled) + "\n"))
					}
					_ = conn.Close()
				}
			}
			break
		} else if d.status == JOINED {
			dest := d.MemberList.FindGossipDest(ADDED)
			if dest != "" {
				d.Heartbeat(false)
				message := d.MakeMessage(dest)
				conn, err := net.Dial("tcp", dest+":"+d.config.Port)
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
		time.Sleep(d.config.GossipInterval * time.Millisecond)
	}
	defer func() { d.LeaveChannel <- "OK" }()
}

func (d *MemberInfo) Listen() {
	fmt.Println("Start to listen")
	ln, err := net.Listen("tcp", ":"+d.config.Port)
	if err != nil {
		log.Fatal(err)
	}
	for ; d.status == JOINED; {
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
			d.MemberList.UpdateMembership(struct_msg)
			mux.Unlock()
		}
		_ = conn.Close()
	}
}

func (d *MemberInfo) Leave() {
	d.status = LEAVING
	log.Println("Leaving...")
	<-d.LeaveChannel
	log.Println("Left")
}

func (d *MemberInfo) Run() {
	go d.Gossip()
	go d.Listen()
	fmt.Println("MemberInfo running...")
}
