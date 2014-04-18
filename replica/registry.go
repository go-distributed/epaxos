package replica

import (
	"fmt"
	"reflect"

	"github.com/go-distributed/epaxos/data"
)

var registry map[uint8]reflect.Type

func init() {
	registry = make(map[uint8]reflect.Type)
	registerMsgType(data.ProposeMsg, data.Propose{})
	registerMsgType(data.PreAcceptMsg, data.PreAccept{})
	registerMsgType(data.PreAcceptOkMsg, data.PreAcceptOk{})
	registerMsgType(data.PreAcceptReplyMsg, data.PreAcceptReply{})
	registerMsgType(data.AcceptMsg, data.Accept{})
	registerMsgType(data.AcceptReplyMsg, data.AcceptReply{})
	registerMsgType(data.CommitMsg, data.Commit{})
	registerMsgType(data.PrepareMsg, data.Prepare{})
	registerMsgType(data.PrepareReplyMsg, data.PrepareReply{})
}

func registerMsgType(typ uint8, msg interface{}) {
	registry[typ] = reflect.TypeOf(msg)
}

func messageProto(typ uint8) (Message, error) {
	t, ok := registry[typ]
	if !ok {
		return nil, fmt.Errorf("unknown message type")
	}
	v := reflect.New(t)
	msg := v.Interface().(Message)
	return msg, nil
}
