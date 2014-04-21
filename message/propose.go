package message

import (
	"fmt"
)

type Propose struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Created    chan struct{}
}

func NewPropose(rid uint8, iid uint64, cmds Commands) *Propose {
	return &Propose{
		ReplicaId:  rid,
		InstanceId: iid,
		Cmds:       cmds,
		Created:    make(chan struct{}),
	}
}

func (p *Propose) Type() uint8 {
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
