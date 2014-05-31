package message

import (
	"fmt"

	"github.com/go-distributed/epaxos/protobuf"
	"github.com/golang/glog"
)

type PreAccept struct {
	ReplicaId  uint8
	InstanceId uint64
	Cmds       Commands
	Deps       Dependencies
	Ballot     *Ballot
	From       uint8
	pb         protobuf.PreAccept // for protobuf
}

// we don't need ReplicaId in PreAcceptOk,
// because only the leader will receive this message.
type PreAcceptOk struct {
	ReplicaId  uint8
	InstanceId uint64
	From       uint8
	pb         protobuf.PreAcceptOK // for protobuf
}

type PreAcceptReply struct {
	ReplicaId  uint8
	InstanceId uint64
	Deps       Dependencies
	Ballot     *Ballot
	From       uint8
	pb         protobuf.PreAcceptReply
}

// PreAccept
func (p *PreAccept) Sender() uint8 {
	return p.From
}

func (p *PreAccept) Type() uint8 {
	return PreAcceptMsg
}

func (p *PreAccept) Content() interface{} {
	return p
}

func (p *PreAccept) Replica() uint8 {
	return p.ReplicaId
}

func (p *PreAccept) Instance() uint64 {
	return p.InstanceId
}

func (p *PreAccept) String() string {
	return fmt.Sprintf("PreAccept, Instance[%v][%v], Ballot[%v]", p.ReplicaId, p.InstanceId, p.Ballot.String())
}

func (p *PreAccept) MarshalBinary() ([]byte, error) {
	replicaID := uint32(p.ReplicaId)
	instanceID := uint64(p.InstanceId)
	from := uint32(p.From)

	p.pb.ReplicaID = &replicaID
	p.pb.InstanceID = &instanceID
	p.pb.Cmds = p.Cmds.ToBytesSlice()
	p.pb.Deps = p.Deps
	p.pb.Ballot = p.Ballot.ToProtobuf()
	p.pb.From = &from

	data, err := p.pb.Marshal()
	if err != nil {
		glog.Warning("PreAccept: MarshalBinary() error: ", err)
		return nil, err
	}
	return data, nil
}

func (p *PreAccept) UnmarshalBinary(data []byte) error {
	if err := p.pb.Unmarshal(data); err != nil {
		glog.Warning("PreAccept: UnmarshalBinary() error: ", err)
		return err
	}

	p.ReplicaId = uint8(*p.pb.ReplicaID)
	p.InstanceId = uint64(*p.pb.InstanceID)
	p.Cmds.FromBytesSlice(p.pb.Cmds)
	p.Deps = p.pb.Deps
	if p.Ballot == nil {
		p.Ballot = new(Ballot)
	}
	p.Ballot.FromProtobuf(p.pb.Ballot)
	p.From = uint8(*p.pb.From)
	return nil
}

// PreAcceptOk
func (p *PreAcceptOk) Sender() uint8 {
	return p.From
}

func (p *PreAcceptOk) Type() uint8 {
	return PreAcceptOkMsg
}

func (p *PreAcceptOk) Content() interface{} {
	return p
}

func (p *PreAcceptOk) Replica() uint8 {
	return p.ReplicaId
}

func (p *PreAcceptOk) Instance() uint64 {
	return p.InstanceId
}

func (p *PreAcceptOk) String() string {
	return fmt.Sprintf("PreAcceptOk, Instance[%v][%v]", p.ReplicaId, p.InstanceId)
}

func (p *PreAcceptOk) MarshalBinary() ([]byte, error) {
	replicaID := uint32(p.ReplicaId)
	instanceID := uint64(p.InstanceId)
	from := uint32(p.From)

	p.pb.ReplicaID = &replicaID
	p.pb.InstanceID = &instanceID
	p.pb.From = &from

	data, err := p.pb.Marshal()
	if err != nil {
		glog.Warning("PreAcceptOk: MarshalBinary() error: ", err)
		return nil, err
	}
	return data, nil
}

func (p *PreAcceptOk) UnmarshalBinary(data []byte) error {
	if err := p.pb.Unmarshal(data); err != nil {
		glog.Warning("PreAcceptOk: UnmarshalBinary() error: ", err)
		return err
	}

	p.ReplicaId = uint8(*p.pb.ReplicaID)
	p.InstanceId = uint64(*p.pb.InstanceID)
	p.From = uint8(*p.pb.From)
	return nil
}

// PreAcceptReply
func (p *PreAcceptReply) Sender() uint8 {
	return p.From
}

func (p *PreAcceptReply) Type() uint8 {
	return PreAcceptReplyMsg
}

func (p *PreAcceptReply) Content() interface{} {
	return p
}

func (p *PreAcceptReply) Replica() uint8 {
	return p.ReplicaId
}

func (p *PreAcceptReply) Instance() uint64 {
	return p.InstanceId
}

func (p *PreAcceptReply) String() string {
	return fmt.Sprintf("PreAcceptReply, Instance[%v][%v], Ballot[%v]", p.ReplicaId, p.InstanceId, p.Ballot.String())
}

func (p *PreAcceptReply) MarshalBinary() ([]byte, error) {
	replicaID := uint32(p.ReplicaId)
	instanceID := uint64(p.InstanceId)
	from := uint32(p.From)

	p.pb.ReplicaID = &replicaID
	p.pb.InstanceID = &instanceID
	p.pb.Deps = p.Deps
	p.pb.Ballot = p.Ballot.ToProtobuf()
	p.pb.From = &from

	data, err := p.pb.Marshal()
	if err != nil {
		glog.Warning("PreAcceptReply: MarshalBinary() error: ", err)
		return nil, err
	}
	return data, nil
}

func (p *PreAcceptReply) UnmarshalBinary(data []byte) error {
	if err := p.pb.Unmarshal(data); err != nil {
		glog.Warning("PreAcceptReply: UnmarshalBinary() error: ", err)
		return err
	}

	p.ReplicaId = uint8(*p.pb.ReplicaID)
	p.InstanceId = uint64(*p.pb.InstanceID)
	p.Deps = p.pb.Deps
	if p.Ballot == nil {
		p.Ballot = new(Ballot)
	}
	p.Ballot.FromProtobuf(p.pb.Ballot)
	p.From = uint8(*p.pb.From)
	return nil
}
