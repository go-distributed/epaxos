package data

type Accept struct {
	cmds       []Command
	seq        int
	deps       Dependencies
	replicaId  int
	instanceId uint64
	ballot     *Ballot
}

type AcceptReply struct {
	ok         bool
	replicaId  uint8
	instanceId uint64
	ballot     *Ballot
	status     int8
}

func (a *Accept) Type() uint8 {
	return acceptType
}

func (a *Accept) Content() interface{} {
	return a
}

func (a *AcceptReply) Type() uint8 {
	return acceptReplyType
}

func (a *AcceptReply) Content() interface{} {
	return a
}
