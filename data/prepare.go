package data

type Prepare struct {
	Ballot          *Ballot
	ReplicaId       uint8
	InstanceId      uint64
	needCmdsInReply bool
}

type PrepareReply struct {
	Ok         bool
	Ballot     *Ballot
	ReplicaId  uint8
	InstanceId uint64
	Status     uint8
	Cmds       Commands
	Deps       Dependencies
}

func (p *Prepare) Type() uint8 {
	return PrepareMsg
}

func (p *Prepare) Content() interface{} {
	return p
}

func (p *PrepareReply) Type() uint8 {
	return PrepareReplyMsg
}

func (p *PrepareReply) Content() interface{} {
	return p
}
