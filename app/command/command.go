package command

// Command from user input
type Command struct {
	Method  string
	Params  []string
}

const (
	Join 		= "join"
	Leave 		= "leave"
	Send 		= "send"
	Switch 		= "switch"
	Display 	= "display"

	Put 		= "put"
	Get 		= "get"
	Delete 		= "delete"
	List 		= "ls"
	Store 		= "store"

	Maple 		= "maple"
	Juice 		= "juice"

	Quit 		= "quit"
)
