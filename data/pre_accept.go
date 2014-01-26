package data

type PreAccept struct {
	Cmds       []Command
	Seq        uint32
	Deps       []uint64
	ReplicaId  uint8
	InstanceId uint64
	Ballot     *Ballot
}

type PreAcceptOk struct {
	InstanceId uint64
}

type PreAcceptReply struct {
	Deps       []uint64
	ReplicaId  uint8
	InstanceId uint64
	Ok         bool
}

func (p *PreAccept) Type() uint8 {
	return preAcceptType
}
func (p *PreAccept) Content() interface{} {
	return p
}
func (p *PreAcceptOk) Type() uint8 {
	return preAcceptOKType
}
func (p *PreAcceptOk) Content() interface{} {
	return p
}
func (p *PreAcceptReply) Type() uint8 {
	return preAcceptReplyType
}
func (p *PreAcceptReply) Content() interface{} {
	return p
}
