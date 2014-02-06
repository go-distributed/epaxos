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
