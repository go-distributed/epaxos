package data

type Commit struct {
	Cmds       Commands
	Seq        uint32
	Deps       Dependencies
	ReplicaId  uint8
	InstanceId uint64
}

func (c *Commit) Type() uint8 {
	return CommitMsg
}

func (c *Commit) Content() interface{} {
	return c
}
