package message

type Message interface {
	Sender() uint8
	Type() MsgType
	Content() interface{}
	Replica() uint8
	Instance() uint64
	String() string
	MarshalProtobuf() ([]byte, error)
	UnmarshalProtobuf([]byte) error
}

// TODO(yifan): Remove this.
func MessageTypeString(m Message) string {
	switch m.Type() {
	case ProposeMsg:
		return "Propose"
	case PreAcceptMsg:
		return "PreAccept"
	case PreAcceptOkMsg:
		return "PreAcceptOk"
	case PreAcceptReplyMsg:
		return "PreAcceptReply"
	case AcceptMsg:
		return "Accept"
	case AcceptReplyMsg:
		return "AcceptReply"
	case CommitMsg:
		return "Commit"
	case PrepareMsg:
		return "Prepare"
	case PrepareReplyMsg:
		return "PrepareReply"
	default:
		panic("")
	}
}

func TypeToString(mtype MsgType) string {
	switch mtype {
	case ProposeMsg:
		return "Propose"
	case PreAcceptMsg:
		return "PreAccept"
	case PreAcceptOkMsg:
		return "PreAcceptOk"
	case PreAcceptReplyMsg:
		return "PreAcceptReply"
	case AcceptMsg:
		return "Accept"
	case AcceptReplyMsg:
		return "AcceptReply"
	case CommitMsg:
		return "Commit"
	case PrepareMsg:
		return "Prepare"
	case PrepareReplyMsg:
		return "PrepareReply"
	default:
		panic("")
	}
}
