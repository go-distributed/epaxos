package data

type PreAccept struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Seq        uint32
	Deps       Dependencies
	Ballot     *Ballot
}

type PreAcceptOk struct {
	InstanceId uint64
}

type PreAcceptReply struct {
	Ok         bool
	ReplicaId  uint8
	InstanceId uint64
	Seq        uint32
	Deps       Dependencies
	Ballot     *Ballot
}

func (p *PreAccept) Type() uint8 {
	return PreAcceptMsg
}
func (p *PreAccept) Content() interface{} {
	return p
}
func (p *PreAcceptOk) Type() uint8 {
	return PreAcceptOkMsg
}
func (p *PreAcceptOk) Content() interface{} {
	return p
}
func (p *PreAcceptReply) Type() uint8 {
	return PreAcceptReplyMsg
}
func (p *PreAcceptReply) Content() interface{} {
	return p
}
