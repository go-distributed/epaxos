package message

import (
	"fmt"

	"github.com/go-distributed/epaxos/protobuf"
	"github.com/golang/glog"
)

type Commit struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Deps       Dependencies
	From       uint8
	pb         protobuf.Commit // for protobuf
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

func (c *Commit) MarshalBinary() ([]byte, error) {
	replicaID := uint32(c.ReplicaId)
	instanceID := uint64(c.InstanceId)
	from := uint32(c.From)

	c.pb.ReplicaID = &replicaID
	c.pb.InstanceID = &instanceID
	c.pb.Cmds = c.Cmds.ToBytesSlice()
	c.pb.Deps = c.Deps
	c.pb.From = &from

	data, err := c.pb.Marshal()
	if err != nil {
		glog.Warning("Commit: MarshalBinary() error: ", err)
		return nil, err
	}
	return data, nil
}

func (c *Commit) UnmarshalBinary(data []byte) error {
	if err := c.pb.Unmarshal(data); err != nil {
		glog.Warning("Commit: UnmarshalBinary() error: ", err)
		return err
	}

	c.ReplicaId = uint8(c.pb.GetReplicaID())
	c.InstanceId = uint64(c.pb.GetInstanceID())
	c.Cmds.FromBytesSlice(c.pb.GetCmds())
	c.Deps = c.pb.GetDeps()
	c.From = uint8(c.pb.GetFrom())
	return nil
}
