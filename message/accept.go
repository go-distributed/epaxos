package message

import (
	"fmt"

	"github.com/go-distributed/epaxos/protobuf"
	"github.com/golang/glog"
)

type Accept struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Deps       Dependencies
	Ballot     *Ballot
	From       uint8
	pb         protobuf.Accept // for protobuf
}

type AcceptReply struct {
	ReplicaId  uint8
	InstanceId uint64
	Ballot     *Ballot
	From       uint8
	pb         protobuf.AcceptReply // for protobuf
}

func (a *Accept) Sender() uint8 {
	return a.From
}

func (a *Accept) Type() MsgType {
	return AcceptMsg
}

func (a *Accept) Content() interface{} {
	return a
}

func (a *Accept) Replica() uint8 {
	return a.ReplicaId
}

func (a *Accept) Instance() uint64 {
	return a.InstanceId
}

func (a *Accept) String() string {
	return fmt.Sprintf("Accept, Instance[%v][%v], Ballot[%v]",
		a.ReplicaId, a.InstanceId, a.Ballot.String())
}

func (a *Accept) MarshalProtobuf() ([]byte, error) {
	replicaID := uint32(a.ReplicaId)
	instanceID := uint64(a.InstanceId)
	from := uint32(a.From)

	a.pb.ReplicaID = &replicaID
	a.pb.InstanceID = &instanceID
	a.pb.Cmds = a.Cmds.ToBytesSlice()
	a.pb.Deps = a.Deps
	a.pb.Ballot = a.Ballot.ToProtobuf()
	a.pb.From = &from

	data, err := a.pb.Marshal()
	if err != nil {
		glog.Warning("Accept: MarshalProtobuf() error: ", err)
		return nil, err
	}
	return data, nil
}

func (a *Accept) UnmarshalProtobuf(data []byte) error {
	if err := a.pb.Unmarshal(data); err != nil {
		glog.Warning("Accept: UnmarshalProtobuf() error: ", err)
		return err
	}

	a.ReplicaId = uint8(a.pb.GetReplicaID())
	a.InstanceId = uint64(a.pb.GetInstanceID())
	a.Cmds.FromBytesSlice(a.pb.GetCmds())
	a.Deps = a.pb.GetDeps()
	if a.Ballot == nil {
		a.Ballot = new(Ballot)
	}
	a.Ballot.FromProtobuf(a.pb.GetBallot())
	a.From = uint8(a.pb.GetFrom())
	return nil
}

func (a *AcceptReply) Sender() uint8 {
	return a.From
}

func (a *AcceptReply) Type() MsgType {
	return AcceptReplyMsg
}

func (a *AcceptReply) Content() interface{} {
	return a
}

func (a *AcceptReply) Replica() uint8 {
	return a.ReplicaId
}

func (a *AcceptReply) Instance() uint64 {
	return a.InstanceId
}

func (a *AcceptReply) String() string {
	return fmt.Sprintf("AcceptReply, Instance[%v][%v], Ballot[%v]", a.ReplicaId, a.InstanceId, a.Ballot.String())
}

func (a *AcceptReply) MarshalProtobuf() ([]byte, error) {
	replicaID := uint32(a.ReplicaId)
	instanceID := uint64(a.InstanceId)
	from := uint32(a.From)

	a.pb.ReplicaID = &replicaID
	a.pb.InstanceID = &instanceID
	a.pb.Ballot = a.Ballot.ToProtobuf()
	a.pb.From = &from

	data, err := a.pb.Marshal()
	if err != nil {
		glog.Warning("AcceptReply: MarshalProtobuf() error: ", err)
		return nil, err
	}
	return data, nil
}

func (a *AcceptReply) UnmarshalProtobuf(data []byte) error {
	if err := a.pb.Unmarshal(data); err != nil {
		glog.Warning("AcceptReply: UnmarshalProtobuf() error: ", err)
		return err
	}

	a.ReplicaId = uint8(a.pb.GetReplicaID())
	a.InstanceId = uint64(a.pb.GetInstanceID())
	if a.Ballot == nil {
		a.Ballot = new(Ballot)
	}
	a.Ballot.FromProtobuf(a.pb.GetBallot())
	a.From = uint8(a.pb.GetFrom())
	return nil
}
