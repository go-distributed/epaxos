package data

type Accept struct {
	Cmds       Commands
	Seq        uint32
	Deps       Dependencies
	ReplicaId  uint8
	InstanceId uint64
	Ballot     *Ballot
}

type AcceptReply struct {
	Ok         bool
	ReplicaId  uint8
	InstanceId uint64
	Ballot     *Ballot
	Status     uint8
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
