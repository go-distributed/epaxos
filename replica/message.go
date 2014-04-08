package replica

import (
	"github.com/go-distributed/epaxos/data"
)

type Message interface {
	Type() uint8
	Content() interface{}
	Replica() uint8
	Instance() uint64
	String() string
}

func MessageTypeString(m Message) string {
	switch m.Type() {
	case data.ProposeMsg:
		return "Propose"
	case data.PreAcceptMsg:
		return "PreAccept"
	case data.PreAcceptOkMsg:
		return "PreAcceptOk"
	case data.PreAcceptReplyMsg:
		return "PreAcceptReply"
	case data.AcceptMsg:
		return "Accept"
	case data.AcceptReplyMsg:
		return "AcceptReply"
	case data.CommitMsg:
		return "Commit"
	case data.PrepareMsg:
		return "Prepare"
	case data.PrepareReplyMsg:
		return "PrepareReply"
	default:
		panic("")
	}
}
