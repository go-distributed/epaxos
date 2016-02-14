package codec

import (
	"bytes"
	"fmt"

	"github.com/go-distributed/epaxos/message"
	"github.com/golang/glog"
)

// The gogoprotobuf codec.
type GoGoProtobufCodec struct{}

// Create a new gogpprotobuf codec.
func NewGoGoProtobuCodec() (*GoGoProtobufCodec, error) {
	return &GoGoProtobufCodec{}, nil
}

// Initial the gogoprotobuf codec (no-op for now).
func (gc *GoGoProtobufCodec) Initial() error {
	return nil
}

// Stop the gogoprotobuf codec (no-op for now).
func (gc *GoGoProtobufCodec) Stop() error {
	return nil
}

// Destroy the gogoprotobuf codec (no-op for now).
func (gc *GoGoProtobufCodec) Destroy() error {
	return nil
}

// Marshal a message into a byte slice.
func (gc *GoGoProtobufCodec) Marshal(msg message.Message) ([]byte, error) {
	b, err := msg.MarshalProtobuf()
	if err != nil {
		glog.Warning("GoGoProtobufCodec: Failed to Marshal: ", err)
		return nil, err
	}

	// Use bytes.Buffer to write efficiently.
	var buf bytes.Buffer
	if err = buf.WriteByte(byte(msg.Type())); err != nil {
		glog.Warning("GoGoProtobufCodec: Failed to Marshal: ", err)
		return nil, err
	}

	n, err := buf.Write(b)
	if err != nil || n != len(b) {
		glog.Warning("GoGoProtobufCodec: Failed to Marshal: ", err)
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal a message from a byte slice.
func (c *GoGoProtobufCodec) Unmarshal(data []byte) (message.Message, error) {
	var msg message.Message

	buf := bytes.NewBuffer(data)
	bt, err := buf.ReadByte()
	if err != nil {
		glog.Warning("GoGoProtobufCodec: Failed to Unmarshal: ", err)
		return nil, err
	}

	mtype := message.MsgType(bt)
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
		err := fmt.Errorf("Unknown message type %s\n", message.TypeToString(mtype))
		glog.Warning("GoGoProtobufCodec: Failed to Unmarshal: ", err)
		return nil, err
	}

	// TODO(yifan): Move this from the message package
	// to the protobuf package.
	if err := msg.UnmarshalProtobuf(buf.Bytes()); err != nil {
		glog.Warning("GoGoProtobufCodec: Failed to Unmarshal: ", err)
		return nil, err
	}
	return msg, nil
}
