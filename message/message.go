package message

type Message interface {
	Sender() uint8
	Type() uint8
	Content() interface{}
	Replica() uint8
	Instance() uint64
	String() string
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
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
