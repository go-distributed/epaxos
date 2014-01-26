package data

type Commit struct {
	Cmds       []Command
	Seq        int
	Deps       []uint64
	ReplicaId  int
	InstanceId uint64
}

func (c *Commit) Type() uint8 {
	return commitType
}

func (c *Commit) Content() interface{} {
	return c
}
