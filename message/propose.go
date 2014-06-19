package message

import (
	"fmt"
)

type Propose struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Created    chan struct{}
	From       uint8
}

func NewPropose(rid uint8, iid uint64, cmds Commands) *Propose {
	return &Propose{
		ReplicaId:  rid,
		InstanceId: iid,
		Cmds:       cmds,
		Created:    make(chan struct{}),
		From:       rid,
	}
}

func (p *Propose) Sender() uint8 {
	return p.From
}

func (p *Propose) Type() MsgType {
	return ProposeMsg
}
func (p *Propose) Content() interface{} {
	return p
}

func (p *Propose) Replica() uint8 {
	return p.ReplicaId
}

func (p *Propose) Instance() uint64 {
	return p.InstanceId
}

func (p *Propose) String() string {
	return fmt.Sprintf("Propose, Instance[%v][%v]", p.ReplicaId, p.InstanceId)
}

func (p *Propose) MarshalProtobuf() ([]byte, error) {
	return nil, fmt.Errorf("Propose: MarshalProtobuf() not implemented\n")
}

func (p *Propose) UnmarshalProtobuf([]byte) error {
	return fmt.Errorf("Propose: UnmarshalProtobuf() not implemented\n")
}
