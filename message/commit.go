package message

import (
	"fmt"
)

type Commit struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Deps       Dependencies
	From       uint8
}

func (c *Commit) Sender() uint8 {
	return c.From
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

func (c *Commit) String() string {
	return fmt.Sprintf("Commit, Instance[%v][%v]", c.ReplicaId, c.InstanceId)
}
