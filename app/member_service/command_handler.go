package member_service

import (
	"better_mp3/app/command"
	"better_mp3/app/logger"
)

func (ms *MemberServer) HandleSwitch(command command.Command) {
	var param string
	if len(command.Params) >= 1 {
		param = command.Params[0]
	} else {
		param = ""
	}

	ms.ChangeStrategy(param)
}

func (ms *MemberServer) HandleDisplay(command command.Command) {
	var param string
	if len(command.Params) >= 1 {
		param = command.Params[0]
	} else {
		param = ""
	}

	if param == "member" {
		if ms.localMessage != nil {
			ms.mux.Lock()
			logger.PrintToConsole(
				"Printing membership list:\n",
				ms.GetMembershipListString(ms.localMessage, ms.failureList))
			ms.mux.Unlock()
		} else {
			logger.PrintInfo("Membership list is nil")
		}
	} else if param == "self" {
		if ms.SelfID == "" {
			logger.PrintInfo("selfID is non-existent")
		} else {
			logger.PrintToConsole(ms.SelfID)
		}
	} else {
		logger.PrintError("Invalid argument to 'list':", param)
	}
}

func (ms *MemberServer) HandleJoin(command command.Command) {
	var param string
	if len(command.Params) >= 1 {
		param = command.Params[0]
	} else {
		param = ""
	}

	if param == "" {
		logger.PrintInfo("Please specify introducer IP address for joining")
	} else if !ms.isSending {
		ms.LeaderIP = param
		ms.initMembershipList(true)
		ms.isJoining = true
		ms.isSending = true
		go ms.startHeartbeat()
		logger.PrintInfo("Successfully sent join request")
	} else {
		logger.PrintError("Cannot join, already actively sending")
	}
}

func (ms *MemberServer) HandleLeave(command command.Command) {
	ms.sendLeaveRequest()
}
