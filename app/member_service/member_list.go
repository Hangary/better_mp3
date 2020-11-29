package member_service

import (
	"better_mp3/app/config"
	"better_mp3/app/logger"
	"better_mp3/app/member_service/protocol_buffer"
	"cs425_mp2/util"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/copier"

)

func (ms *MemberServer) initMembershipList(isGossip bool) {
	selfMember := protocol_buffer.Member{
		HeartbeatCounter: 1,
		LastSeen:         ptypes.TimestampNow(),
	}

	strat := config.STRAT_GOSSIP

	if !isGossip {
		strat = config.STRAT_ALL
	}

	ms.localMessage = &protocol_buffer.MembershipServiceMessage{
		MemberList:      make(map[string]*protocol_buffer.Member),
		Strategy:        strat,
		StrategyCounter: 1,
	}

	if ms.IsLeader {
		ms.localMessage.Type = protocol_buffer.MessageType_STANDARD
	} else {
		ms.localMessage.Type = protocol_buffer.MessageType_JOINREQ
	}

	localIP := util.GetLocalIPAddr().String()
	ms.SelfID = localIP + ":" + ptypes.TimestampString(selfMember.LastSeen)

	ms.AddMemberToMembershipList(ms.localMessage, ms.SelfID, &selfMember)
}

// MergeMembershipLists : merge remote membership list into local membership list
func (ms *MemberServer) mergeMembershipLists(localMessage, remoteMessage *protocol_buffer.MembershipServiceMessage, failureList map[string]bool) *protocol_buffer.MembershipServiceMessage {
	if remoteMessage.StrategyCounter > localMessage.StrategyCounter {
		localMessage.Strategy = remoteMessage.Strategy
		localMessage.StrategyCounter = remoteMessage.StrategyCounter
		logger.PrintInfo("Received request to change system strategy to", localMessage.Strategy)
	}

	for machineID, member := range remoteMessage.MemberList {
		if _, ok := localMessage.MemberList[machineID]; !ok {
			if remoteMessage.MemberList[machineID].IsLeaving {
				break
			}

			memberCpy := protocol_buffer.Member{}
			err := copier.Copy(&memberCpy, &member)
			if err != nil {
				logger.PrintError("Error when copying: ", err)
			}
			ms.AddMemberToMembershipList(localMessage, machineID, &memberCpy)
			continue
		} else if remoteMessage.MemberList[machineID].IsLeaving {
			localMessage.MemberList[machineID].IsLeaving = true
		}

		remoteHeartBeat := remoteMessage.MemberList[machineID].HeartbeatCounter

		if localMessage.MemberList[machineID].HeartbeatCounter < remoteHeartBeat {
			delete(failureList, machineID)
			localMessage.MemberList[machineID].HeartbeatCounter = remoteHeartBeat
			localMessage.MemberList[machineID].LastSeen = ptypes.TimestampNow()
		}
	}
	return localMessage
}

// GetOtherMembershipListIPs : Expecting MachineID to be in format IP:timestamp
func GetOtherMembershipListIPs(message *protocol_buffer.MembershipServiceMessage, selfID string) []string {
	ips := make([]string, 0, len(message.MemberList))

	for machineID := range message.MemberList {
		splitRes := strings.Split(machineID, ":")
		if machineID != selfID {
			ips = append(ips, splitRes[0])
		}
	}

	return ips
}

// CheckAndRemoveMembershipListFailures : Upon sending of membership list mark failures and remove failed machines
func (ms *MemberServer) CheckAndRemoveMembershipListFailures(message *protocol_buffer.MembershipServiceMessage, failureList *map[string]bool) {
	for machineID, member := range message.MemberList {
		timeElapsedSinceLastSeen := float64(ptypes.TimestampNow().GetSeconds() - member.LastSeen.GetSeconds())

		if timeElapsedSinceLastSeen >= config.T_TIMEOUT+config.T_CLEANUP {
			delete(*failureList, machineID)
			ms.RemoveMemberFromMembershipList(message, machineID)
		} else if !(*failureList)[machineID] && timeElapsedSinceLastSeen >= config.T_TIMEOUT {
			(*failureList)[machineID] = true
			logger.PrintInfo("Marking machine", machineID, "as failed")
			ms.HandleMemberFailure(machineID)
		}
	}
}

// AddMemberToMembershipList : add new member to membership list
func (ms *MemberServer) AddMemberToMembershipList(message *protocol_buffer.MembershipServiceMessage, machineID string, member *protocol_buffer.Member) {
	message.MemberList[machineID] = member
	ms.JoinedNodeChan <- strings.Split(machineID, ":")[0]
	logger.PrintInfo("Adding machine", machineID, "to membership list")
}

// RemoveMemberFromMembershipList : remove member from membership list
func (ms *MemberServer) RemoveMemberFromMembershipList(message *protocol_buffer.MembershipServiceMessage, machineID string) {
	delete(message.MemberList, machineID)
	logger.PrintInfo("Removing machine", machineID, "from membership list")
}

func (ms *MemberServer) GetMembershipListString(message *protocol_buffer.MembershipServiceMessage, failureList map[string]bool) string {
	var sb strings.Builder

	machineIDs := make([]string, 0)
	for k := range message.MemberList {
		machineIDs = append(machineIDs, k)
	}

	sort.Strings(machineIDs)

	for _, machineID := range machineIDs {
		if failureList[machineID] {
			sb.WriteString("FAILED:")
		}
		sb.WriteString(machineID +
			" - { HeartbeatCounter: " +
			strconv.Itoa(int(message.MemberList[machineID].HeartbeatCounter)) +
			", LastSeen: " +
			ptypes.TimestampString(message.MemberList[machineID].LastSeen) +
			" }\n")
	}

	sb.WriteString("\n")

	return sb.String()
}
