package data

type Commit struct {
	cmds       []cmd.Command
	seq        int
	deps       []InstanceId
	replicaId  int
	instanceId InstanceId
}

func (c *Commit) Type() uint8 {
	return commitType
}

func (c *Commit) Content() *Commit {
	return c
}