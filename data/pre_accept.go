package data

type PreAccept struct {
	cmds       []Command
	seq        uint32
	deps       []uint64
	replicaId  int
	instanceId uint64
	ballot     *Ballot
}

func (*PreAccept) Type() uint8 {
        return preAcceptType
}
