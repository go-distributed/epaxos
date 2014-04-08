package data

import (
	"fmt"
)

type PreAccept struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Deps       Dependencies
	Ballot     *Ballot
}

// we don't need ReplicaId in PreAcceptOk,
// because only the leader will receive this message.
type PreAcceptOk struct {
	ReplicaId  uint8
	InstanceId uint64
}

type PreAcceptReply struct {
	Ok         bool
	ReplicaId  uint8
	InstanceId uint64
	Deps       Dependencies
	Ballot     *Ballot
}

// PreAccept
func (p *PreAccept) Type() uint8 {
	return PreAcceptMsg
}

func (p *PreAccept) Content() interface{} {
	return p
}

func (p *PreAccept) Replica() uint8 {
	return p.ReplicaId
}

func (p *PreAccept) Instance() uint64 {
	return p.InstanceId
}

func (p *PreAccept) String() string {
	return fmt.Sprintf("PreAccept, Instance[%v][%v]", p.ReplicaId, p.InstanceId)
}

// PreAcceptOk
func (p *PreAcceptOk) Type() uint8 {
	return PreAcceptOkMsg
}

func (p *PreAcceptOk) Content() interface{} {
	return p
}

func (p *PreAcceptOk) Replica() uint8 {
	return p.ReplicaId
}

func (p *PreAcceptOk) Instance() uint64 {
	return p.InstanceId
}

func (p *PreAcceptOk) String() string {
	return fmt.Sprintf("PreAcceptOk, Instance[%v][%v]", p.ReplicaId, p.InstanceId)
}

// PreAcceptReply
func (p *PreAcceptReply) Type() uint8 {
	return PreAcceptReplyMsg
}

func (p *PreAcceptReply) Content() interface{} {
	return p
}

func (p *PreAcceptReply) Replica() uint8 {
	return p.ReplicaId
}

func (p *PreAcceptReply) Instance() uint64 {
	return p.InstanceId
}

func (p *PreAcceptReply) String() string {
	return fmt.Sprintf("PreAcceptReply, Instance[%v][%v]", p.ReplicaId, p.InstanceId)
}
