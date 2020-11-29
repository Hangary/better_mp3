package member_service

/*
This file is used to handle a failure of node, especially, the failure of the master.
Once the master is detected to be failed, the member service would wait and then begin an election.
The election is implemented simply by choosing the node with biggest unique ID.
 */

import (
	"better_mp3/app/config"
	"better_mp3/app/logger"
	"strings"
	"time"
)

// MachineID to be in format IP:timestamp
func (ms *MemberServer) HandleMemberFailure(machineID string) {
	ip := strings.Split(machineID, ":")[0]
	ms.FailedNodeChan <- ip
	if ip == ms.LeaderIP {
		logger.PrintInfo("Master is down. Please waiting for electing a new Master...")
		go ms.Election()
	}
}

func (ms *MemberServer) Election() {
	time.Sleep(config.WaitTimeForElection * time.Second)
	logger.PrintInfo("Begin electing a new master:")
	newMasterID := ms.getLargestAliveServer()
	if ms.SelfID == newMasterID {
		logger.PrintInfo("This server has been elected as the new master.")
		ms.IsLeader = true
	} else {
		logger.PrintInfo("New master is selected:", newMasterID)
	}
	ms.LeaderIP = strings.Split(newMasterID, ":")[0]
	// notify the file service
	ms.MasterChanged <- 1
}

func (ms *MemberServer) getLargestAliveServer() string {
	largestID := ms.SelfID
	for machineID, member := range ms.localMessage.MemberList {
		if !ms.failureList[machineID] && !member.IsLeaving {
			if strings.Compare(machineID, largestID) > 0 {
				largestID = machineID
			}
		}
	}
	return largestID
}