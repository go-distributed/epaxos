package data

type PreAccept struct {
	cmds       []Command
	seq        uint32
	deps       []uint64
	replicaId  int
	instanceId uint64
	ballot     *Ballot
}

type PreAcceptOk struct {
	instanceId uint64
}

type PreAcceptReply struct {
	deps       []uint64
	replicaId  int
	instanceId uint64
	ok         bool
}

func (p *PreAccept) Type() uint8 {
	return preAcceptType
}
func (p *PreAccept) Content() interface{} {
	return p
}
func (*PreAcceptOk) Type() uint8 {
	return preAcceptOKType
}
func (p *PreAcceptOk) Content() interface{} {
	return p
}
func (*PreAcceptReply) Type() uint8 {
	return preAcceptReplyType
}
func (p *PreAcceptReply) Content() interface{} {
	return p
}
