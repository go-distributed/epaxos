package message

type MsgType uint8

const (
	ProposeMsg MsgType = iota + 1
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
