package message

const (
	ProposeMsg uint8 = iota + 1
	PreAcceptMsg
	PreAcceptOkMsg
	PreAcceptReplyMsg
	AcceptMsg
	AcceptReplyMsg
	CommitMsg
	PrepareMsg
	PrepareReplyMsg
	TimeoutMsg
)
