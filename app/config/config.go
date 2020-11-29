package config

var DebugMode = false

var	MemberServicePort = "7008"

const BUFFER_SIZE int = 8192

const (
	T_TIMEOUT           = 5
	T_CLEANUP           = 40
	WaitTimeForElection = 10
	PULSE_TIME = 500
	GOSSIP_FANOUT = 5
)

const STRAT_GOSSIP = "gossip"
const STRAT_ALL = "all"


const PERM_MODE = 0777

