package data

import (
	"fmt"
)

type Prepare struct {
	ReplicaId  uint8
	InstanceId uint64
	Ballot     *Ballot
}

type PrepareReply struct {
	ReplicaId  uint8
	InstanceId uint64
	Status     uint8
	Cmds       Commands
	Deps       Dependencies
	Ballot     *Ballot
	// These two are used for identical non orignal leader pre-accept reply
	OriginalBallot *Ballot
	IsFromLeader   bool
}

func (p *Prepare) Type() uint8 {
	return PrepareMsg
}

func (p *Prepare) Content() interface{} {
	return p
}

func (p *Prepare) Replica() uint8 {
	return p.ReplicaId
}

func (p *Prepare) Instance() uint64 {
	return p.InstanceId
}

func (p *Prepare) String() string {
	return fmt.Sprintf("Prepare, Instance[%v][%v], Ballot[%v]", p.ReplicaId, p.InstanceId, p.Ballot.String())
}

func (p *PrepareReply) Type() uint8 {
	return PrepareReplyMsg
}

func (p *PrepareReply) Content() interface{} {
	return p
}

func (p *PrepareReply) Replica() uint8 {
	return p.ReplicaId
}

func (p *PrepareReply) Instance() uint64 {
	return p.InstanceId
}

func (p *PrepareReply) String() string {
	return fmt.Sprintf("PrepareReply, Instance[%v][%v], Ballot[%v], Original Ballot[%v]",
		p.ReplicaId, p.InstanceId, p.Ballot.String(), p.OriginalBallot.String())
}
