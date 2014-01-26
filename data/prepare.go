package data

type Prepare struct {
	Ballot     *Ballot
	ReplicaId  int
	InstanceId uint64
}

type PrepareReply struct {
	Ok         bool
	Ballot     *Ballot
	ReplicaId  int
	InstanceId uint64
	Status     int8
	Cmds       []Command
	Deps       []uint64
}

func (p *Prepare) Type() uint8 {
	return prepareType
}

func (p *Prepare) Content() interface{} {
	return p
}

func (p *PrepareReply) Type() uint8 {
	return prepareReplyType
}

func (p *PrepareReply) Content() interface{} {
	return p
}
