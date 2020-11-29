package member_service

import (
	"better_mp3/app/config"
	"better_mp3/app/logger"
	"better_mp3/app/member_service/protocol_buffer"
	"math/rand"
	"net"
	"time"



	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

var BytesSent int = 0
var MessageLossRate float64 = -1

func EncodeMembershipServiceMessage(serviceMessage *protocol_buffer.MembershipServiceMessage) ([]byte, error) {
	message, err := proto.Marshal(serviceMessage)

	return message, err
}

func DecodeMembershipServiceMessage(message []byte) (*protocol_buffer.MembershipServiceMessage, error) {
	list := &protocol_buffer.MembershipServiceMessage{}
	err := proto.Unmarshal(message, list)

	return list, err
}

func SendGossip(serviceMessage *protocol_buffer.MembershipServiceMessage, k int, selfID string) error {
	message, err := EncodeMembershipServiceMessage(serviceMessage)
	if err != nil {
		return err
	}

	dests := GetOtherMembershipListIPs(serviceMessage, selfID)

	if k < len(serviceMessage.MemberList) {
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(dests), func(i, j int) {
			dests[i], dests[j] = dests[j], dests[i]
		})

		dests = dests[:k]
	}

	return SendAll(dests, message)
}

func SendHeartbeat(fullMessage, selfMessage *protocol_buffer.MembershipServiceMessage, selfID string) error {
	dests := GetOtherMembershipListIPs(fullMessage, selfID)

	message, err := EncodeMembershipServiceMessage(selfMessage)
	if err != nil {
		return err
	}

	return SendAll(dests, message)
}

func HeartbeatGossip(serviceMessage *protocol_buffer.MembershipServiceMessage, k int, selfID string) error {
	return SendGossip(serviceMessage, k, selfID)
}

func HeartbeatAllToAll(serviceMessage *protocol_buffer.MembershipServiceMessage, selfID string) error {
	selfMessage := &protocol_buffer.MembershipServiceMessage{
		MemberList:      make(map[string]*protocol_buffer.Member),
		Strategy:        serviceMessage.Strategy,
		StrategyCounter: serviceMessage.StrategyCounter,
	}

	selfMember := protocol_buffer.Member{
		HeartbeatCounter: serviceMessage.MemberList[selfID].HeartbeatCounter,
		LastSeen:         ptypes.TimestampNow(),
		IsLeaving:        serviceMessage.MemberList[selfID].IsLeaving,
	}

	selfMessage.MemberList[selfID] = &selfMember

	return SendHeartbeat(serviceMessage, selfMessage, selfID)
}

func SendAll(destinations []string, message []byte) error {
	for _, v := range destinations {
		err := Send(v, message)
		if err != nil {
			return err
		}
	}

	return nil
}

func Send(dest string, message []byte) error {
	if len(message) > config.BUFFER_SIZE {
		logger.WarningLogger.Println("Send: message is larger than BUFFER_SIZE")
	}

	rand.NewSource(time.Now().UnixNano())
	if rand.Float64() > MessageLossRate {
		addr, err := net.ResolveUDPAddr("udp", dest+":"+config.MemberServicePort)
		if err != nil {
			return err
		}

		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			return err
		}

		defer conn.Close()

		_, err = conn.Write(message)
		if err != nil {
			return err
		}

		BytesSent += len(message)
	}

	return nil
}

func Listen(port string, callback func(message []byte) error) error {
	port = ":" + port

	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	defer conn.Close()

	buffer := make([]byte, config.BUFFER_SIZE)
	for {
		n, err := conn.Read(buffer)

		if err != nil {
			return err
		}

		callback(buffer[0:n])
	}
}

func GetLocalIPAddr() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		logger.PrintError("net.Dial")
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}
