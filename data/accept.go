package data

import (
	"fmt"
)

type Accept struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Deps       Dependencies
	Ballot     *Ballot
}

type AcceptReply struct {
	ReplicaId  uint8
	InstanceId uint64
	Ballot     *Ballot
}

func (a *Accept) Type() uint8 {
	return AcceptMsg
}

func (a *Accept) Content() interface{} {
	return a
}

func (p *Accept) Replica() uint8 {
	return p.ReplicaId
}

func (p *Accept) Instance() uint64 {
	return p.InstanceId
}

func (p *Accept) String() string {
	return fmt.Sprintf("Accept, Instance[%v][%v], Ballot[%v]", p.ReplicaId, p.InstanceId, p.Ballot.String())
}

func (a *AcceptReply) Type() uint8 {
	return AcceptReplyMsg
}

func (a *AcceptReply) Content() interface{} {
	return a
}

func (p *AcceptReply) Replica() uint8 {
	return p.ReplicaId
}

func (p *AcceptReply) Instance() uint64 {
	return p.InstanceId
}

func (p *AcceptReply) String() string {
	return fmt.Sprintf("AcceptReply, Instance[%v][%v], Ballot[%v]", p.ReplicaId, p.InstanceId, p.Ballot.String())
}
