package replica

import (
	"fmt"
	"reflect"

	"github.com/go-distributed/epaxos/message"
)

var registry map[uint8]reflect.Type

func init() {
	registry = make(map[uint8]reflect.Type)
	registerMsgType(message.ProposeMsg, message.Propose{})
	registerMsgType(message.PreAcceptMsg, message.PreAccept{})
	registerMsgType(message.PreAcceptOkMsg, message.PreAcceptOk{})
	registerMsgType(message.PreAcceptReplyMsg, message.PreAcceptReply{})
	registerMsgType(message.AcceptMsg, message.Accept{})
	registerMsgType(message.AcceptReplyMsg, message.AcceptReply{})
	registerMsgType(message.CommitMsg, message.Commit{})
	registerMsgType(message.PrepareMsg, message.Prepare{})
	registerMsgType(message.PrepareReplyMsg, message.PrepareReply{})
	registerMsgType(message.TimeoutMsg, message.Timeout{})
}

func registerMsgType(typ uint8, msg interface{}) {
	registry[typ] = reflect.TypeOf(msg)
}

func messageProto(typ uint8) (message.Message, error) {
	t, ok := registry[typ]
	if !ok {
		return nil, fmt.Errorf("unknown message type")
	}
	v := reflect.New(t)
	msg := v.Interface().(message.Message)
	return msg, nil
}
