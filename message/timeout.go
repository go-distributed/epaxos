package message

import (
	"fmt"
)

type Timeout struct {
	ReplicaId  uint8
	InstanceId uint64
	From       uint8
}

func (t *Timeout) Sender() uint8 {
	return t.From
}

func (t *Timeout) Type() MsgType {
	return TimeoutMsg
}

func (t *Timeout) Content() interface{} {
	return t
}

func (t *Timeout) Replica() uint8 {
	return t.ReplicaId
}

func (t *Timeout) Instance() uint64 {
	return t.InstanceId
}

func (t *Timeout) String() string {
	return fmt.Sprintf("Timeout, Instance[%v][%v]", t.ReplicaId, t.InstanceId)
}

func (t *Timeout) MarshalProtobuf() ([]byte, error) {
	return nil, fmt.Errorf("Timeout: MarshalProtobuf() not implemented\n")
}

func (t *Timeout) UnmarshalProtobuf([]byte) error {
	return fmt.Errorf("Timeout: UnmarshalProtobuf() not implemented\n")
}
