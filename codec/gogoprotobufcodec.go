package codec

import (
	"github.com/go-distributed/epaxos/message"
)

type GoGoProtobufCodec struct{}

func NewGoGoProtobufHTTPTransporter() (*GoGoProtobufCodec, error) {
	return new(GoGoProtobufCodec), nil
}

// Initial the gogoprotobuf (no-op for now).
func (gc *GoGoProtobufCodec) Initial() error {
	return nil
}

// Stop the gogoprotobuf (no-op for now).
func (gc *GoGoProtobufCodec) Stop() error {
	return nil
}

// Destroy the gogoprotobuf (no-op for now).
func (gc *GoGoProtobufCodec) Destroy() error {
	return nil
}

// Marshal a message into a byte slice.
func (gc *GoGoProtobufCodec) Marshal(msg message.Message) ([]byte, error) {
	return msg.MarshalProtobuf()
}

// Unmarshal a message from a byte slice.
func (c *GoGoProtobufCodec) Unmarshal(mtype uint8, data []byte) (message.Message, error) {
	var msg message.Message

	switch mtype {
	case message.ProposeMsg:
		msg = new(message.Propose)
	case message.PreAcceptMsg:
		msg = new(message.PreAccept)
	case message.PreAcceptOkMsg:
		msg = new(message.PreAcceptOk)
	case message.PreAcceptReplyMsg:
		msg = new(message.PreAcceptReply)
	case message.AcceptMsg:
		msg = new(message.Accept)
	case message.AcceptReplyMsg:
		msg = new(message.AcceptReply)
	case message.CommitMsg:
		msg = new(message.Commit)
	case message.PrepareMsg:
		msg = new(message.Prepare)
	case message.PrepareReplyMsg:
		msg = new(message.PrepareReply)
	default:
		panic("Unknown message type")
	}

	if err := msg.UnmarshalProtobuf(data); err != nil {
		return nil, err
	}
	return msg, nil
}
