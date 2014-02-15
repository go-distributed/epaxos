package data

type Accept struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Seq        uint32
	Deps       Dependencies
	Ballot     *Ballot
}

type AcceptReply struct {
	Ok         bool
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

func (a *AcceptReply) Type() uint8 {
	return AcceptReplyMsg
}

func (a *AcceptReply) Content() interface{} {
	return a
}

func (p *Accept) Replica() uint8 {
	return p.ReplicaId
}

func (p *Accept) Instance() uint64 {
	return p.InstanceId
}

func (p *AcceptReply) Replica() uint8 {
	return p.ReplicaId
}

func (p *AcceptReply) Instance() uint64 {
	return p.InstanceId
}
