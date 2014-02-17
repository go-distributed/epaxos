package data

type Commit struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Seq        uint32
	Deps       Dependencies
}

func (c *Commit) Type() uint8 {
	return CommitMsg
}

func (c *Commit) Content() interface{} {
	return c
}

func (c *Commit) Replica() uint8 {
	return c.ReplicaId
}

func (c *Commit) Instance() uint64 {
	return c.InstanceId
}
