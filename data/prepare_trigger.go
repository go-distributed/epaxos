package data

import (
	"fmt"
)

type PrepareTrigger struct {
	ReplicaId  uint8
	InstanceId uint64
}

func (p *PrepareTrigger) Type() uint8 {
	return PrepareTriggerMsg
}

func (p *PrepareTrigger) Content() interface{} {
	return p
}

func (p *PrepareTrigger) Replica() uint8 {
	return p.ReplicaId
}

func (p *PrepareTrigger) Instance() uint64 {
	return p.InstanceId
}

func (p *PrepareTrigger) String() string {
	return fmt.Sprintf("PrepareTrigger, Instance[%v][%v]", p.ReplicaId, p.InstanceId)
}
