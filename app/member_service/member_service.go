/*
This package provides member service, including:
	1. membership list
	2. failure detector
	3. simple master election

Credit: This package is adapted from CS425 Fall Recommended MP1 Solutions.
*/
package member_service

import (
	"better_mp3/app/config"
	"better_mp3/app/logger"
	"better_mp3/app/member_service/protocol_buffer"
	"github.com/golang/protobuf/ptypes"
	"sort"
	"strings"
	"sync"
	"time"
)

type MemberServer struct {
	config       config.MemberServiceConfig
	failureList  map[string]bool
	useGossip    bool
	localMessage *protocol_buffer.MembershipServiceMessage
	mux          sync.Mutex

	isSending bool
	isJoining bool

	SelfIP string
	SelfID string

	LeaderIP string
	IsLeader bool

	// these channels are used by upper level service
	FailedNodeChan chan string
	JoinedNodeChan chan string
	MasterChanged  chan int
}

func NewMemberServer() *MemberServer {
	var ms MemberServer

	ms.SelfIP = GetLocalIPAddr()
	ms.config = config.GetMemberServiceConfig()
	ms.LeaderIP = ms.config.IntroducerIP
	ms.IsLeader = ms.SelfIP == ms.config.IntroducerIP
	ms.useGossip = false // todo: change
	ms.MasterChanged = make(chan int)
	ms.isSending = true
	ms.isJoining = !ms.IsLeader
	ms.FailedNodeChan = make(chan string, 10)
	ms.JoinedNodeChan = make(chan string, 10)
	ms.failureList = make(map[string]bool)
	ms.initMembershipList(ms.useGossip)



	return &ms
}

// the entry point of the package, run the member service
func (ms *MemberServer) Run() {
	go Listen(config.MemberServicePort, ms.readNewMessage)
	go ms.startHeartbeat()

	logger.PrintInfo(
		"Member Service is now running\n",
		"\tPort:", config.MemberServicePort,
		"\tIs Master:", ms.IsLeader,
		"\tMasterIP:", ms.LeaderIP,
		"\tIs gossip:", ms.useGossip,
		"\n",
		"\tMember Self ID:", ms.SelfID)
}

/*
	Following methods are exported for other packages so that they can access the membership list
*/

func (ms *MemberServer) GetAliveMemberIPList() []string {
	ipList := make([]string, 0)
	for machineID, member := range ms.localMessage.MemberList {
		if !ms.failureList[machineID] && !member.IsLeaving {
			//if machineID == selfID {
			//	continue
			//}
			ip := strings.Split(machineID, ":")[0]
			ipList = append(ipList, ip)
		}
	}
	sort.Strings(ipList)
	return ipList
}

func (ms *MemberServer) GetFailedMemberIPList() []string {
	failNodes := make([]string, 0)
	for k := range ms.failureList {
		if ms.failureList[k] {
			ip := strings.Split(k, ":")[0]
			failNodes = append(failNodes, ip)
		}
	}
	return failNodes
}

func (ms *MemberServer) ChangeStrategy(input string) {
	if input == config.STRAT_GOSSIP {
		if ms.localMessage.Strategy == config.STRAT_GOSSIP {
			logger.PrintError("System strategy is already gossip")
			return
		}

		ms.localMessage.Strategy = config.STRAT_GOSSIP
	} else if input == config.STRAT_ALL {
		if ms.localMessage.Strategy == config.STRAT_ALL {
			logger.PrintError("System strategy is already all-to-all")
			return
		}

		ms.localMessage.Strategy = config.STRAT_ALL
	} else {
		logger.PrintError("Invalid strategy - must be gossip or all")
		return
	}

	ms.localMessage.StrategyCounter++
	logger.PrintInfo("System strategy successfully changed to", ms.localMessage.Strategy)
}

func (ms *MemberServer) sendLeaveRequest() {
	ms.isSending = false

	ms.mux.Lock()
	ms.localMessage.MemberList[ms.SelfID].IsLeaving = true

	if ms.localMessage.Strategy == config.STRAT_GOSSIP {
		HeartbeatGossip(ms.localMessage, config.GOSSIP_FANOUT, ms.SelfID)
	} else {
		HeartbeatAllToAll(ms.localMessage, ms.SelfID)
	}
	ms.mux.Unlock()

	ms.SelfID = ""
	ms.localMessage = nil
	logger.PrintInfo("Successfully left")
}

func (ms *MemberServer) readNewMessage(message []byte) error {
	if !ms.isSending {
		return nil
	}

	remoteMessage, err := DecodeMembershipServiceMessage(message)
	if err != nil {
		return err
	}
	logger.PrintDebug("Member service received message:", remoteMessage)

	ms.mux.Lock()

	if ms.isJoining && remoteMessage.Type == protocol_buffer.MessageType_JOINREP {
		ms.isJoining = false
		ms.localMessage.Type = protocol_buffer.MessageType_STANDARD
	}

	if !ms.IsLeader && remoteMessage.Type == protocol_buffer.MessageType_JOINREQ {
		ms.mux.Unlock()
		return nil
	}

	logger.PrintDebug("Merging membership list.")
	ms.mergeMembershipLists(ms.localMessage, remoteMessage, ms.failureList)

	if ms.IsLeader && remoteMessage.Type == protocol_buffer.MessageType_JOINREQ {
		logger.PrintInfo("Received a join request.")
		ms.localMessage.Type = protocol_buffer.MessageType_JOINREP
		message, err := EncodeMembershipServiceMessage(ms.localMessage)
		ms.localMessage.Type = protocol_buffer.MessageType_STANDARD

		if err != nil {
			return err
		}

		dests := GetOtherMembershipListIPs(remoteMessage, ms.SelfID)
		Send(dests[0], message)
	}

	ms.mux.Unlock()

	return nil
}

func (ms *MemberServer) startHeartbeat() {
	for ms.isSending {
		ms.mux.Lock()

		ms.localMessage.MemberList[ms.SelfID].LastSeen = ptypes.TimestampNow()
		ms.localMessage.MemberList[ms.SelfID].HeartbeatCounter++
		ms.CheckAndRemoveMembershipListFailures(ms.localMessage, &ms.failureList)
		logger.InfoLogger.Println("Current memberlist:\n", ms.GetMembershipListString(ms.localMessage, ms.failureList), "\n")

		if ms.isJoining {
			message, _ := EncodeMembershipServiceMessage(ms.localMessage)
			Send(ms.LeaderIP, message)
			logger.PrintDebug("Member service sent Message:", ms.localMessage, "to", ms.LeaderIP)
		} else {
			if ms.localMessage.Strategy == config.STRAT_GOSSIP {
				HeartbeatGossip(ms.localMessage, config.GOSSIP_FANOUT, ms.SelfID)
			} else {
				HeartbeatAllToAll(ms.localMessage, ms.SelfID)
			}

			for machineID := range ms.localMessage.MemberList {
				if ms.localMessage.MemberList[machineID].IsLeaving && !ms.failureList[machineID] {
					logger.PrintInfo("Received leave request from machine", machineID)
					ms.failureList[machineID] = true
					ms.FailedNodeChan <- strings.Split(machineID, ":")[0]
				}
			}
		}

		ms.mux.Unlock()

		time.Sleep(config.PULSE_TIME * time.Millisecond)
	}
}
