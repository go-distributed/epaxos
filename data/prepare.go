package data

type Prepare struct {
	ReplicaId       uint8
	InstanceId      uint64
	Ballot          *Ballot
	NeedCmdsInReply bool
}

type PrepareReply struct {
	Ok         bool
	ReplicaId  uint8
	InstanceId uint64
	Status     uint8
	Cmds       Commands
	Deps       Dependencies
	Ballot     *Ballot
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
