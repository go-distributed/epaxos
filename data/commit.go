package data

type Commit struct {
	Cmds       Commands
	Seq        int
	Deps       Dependencies
	ReplicaId  uint8
	InstanceId uint64
}

func (c *Commit) Type() uint8 {
	return commitType
}

func (c *Commit) Content() interface{} {
	return c
}
