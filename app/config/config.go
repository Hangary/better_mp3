package config

import (
	"hash/fnv"
	"time"
)

var DebugMode = false

var (
	MemberServicePort = "8234"
	FileTransferPort  = "8235"
	FileServicePort   = "8236"
)

const (
	SDFS_DIR  = "./FileDir/sdfsFiles/"
	LOCAL_DIR = "./FileDir/localFiles/"
)

const BUFFER_SIZE int = 32768
const REPLICA = 4

const (
	T_TIMEOUT           = 5
	T_CLEANUP           = 40
	WaitTimeForElection = 10
	FileCheckGapSeconds = 3 * time.Second
)

const STRAT_GOSSIP = "gossip"

const STRAT_ALL = "all"

const PULSE_TIME = 500

const GOSSIP_FANOUT = 5

const PERM_MODE = 0777

const (
	PUT    = 4
	GET    = 9
	DELETE = 12
)

func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
