package data

import (
	"fmt"
)

type Propose struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
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
