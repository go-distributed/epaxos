package data

import (
	"fmt"
)

type Timeout struct {
	ReplicaId  uint8
	InstanceId uint64
}

func (t *Timeout) Type() uint8 {
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
