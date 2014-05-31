package message

import (
	"fmt"

	"github.com/go-distributed/epaxos/protobuf"
	"github.com/golang/glog"
)

type Prepare struct {
	ReplicaId  uint8
	InstanceId uint64
	Ballot     *Ballot
	From       uint8
	pb         protobuf.Prepare // for protobuf
}

type PrepareReply struct {
	ReplicaId  uint8
	InstanceId uint64
	Status     uint8
	Cmds       Commands
	Deps       Dependencies
	Ballot     *Ballot
	// These two are used for identical non orignal leader pre-accept reply
	OriginalBallot *Ballot
	IsFromLeader   bool
	From           uint8
	pb             protobuf.PrepareReply // for protobuf
}

func (p *Prepare) Sender() uint8 {
	return p.From
}

func (p *Prepare) Type() uint8 {
	return PrepareMsg
}

func (p *Prepare) Content() interface{} {
	return p
}

func (p *Prepare) Replica() uint8 {
	return p.ReplicaId
}

func (p *Prepare) Instance() uint64 {
	return p.InstanceId
}

func (p *Prepare) String() string {
	return fmt.Sprintf("Prepare, Instance[%v][%v], Ballot[%v]", p.ReplicaId, p.InstanceId, p.Ballot.String())
}

func (p *Prepare) MarshalBinay() ([]byte, error) {
	replicaID := uint32(p.ReplicaId)
	instanceID := uint64(p.InstanceId)
	from := uint32(p.From)

	p.pb.ReplicaID = &replicaID
	p.pb.InstanceID = &instanceID
	p.pb.Ballot = p.Ballot.ToProtobuf()
	p.pb.From = &from

	data, err := p.pb.Marshal()
	if err != nil {
		glog.Warning("Prepare: MarshalBinary() error: ", err)
		return nil, err
	}
	return data, nil
}

func (p *Prepare) UnmarshalBinary(data []byte) error {
	if err := p.pb.Unmarshal(data); err != nil {
		glog.Warning("Prepare: UnmarshalBinary() error: ", err)
		return err
	}

	p.ReplicaId = uint8(*p.pb.ReplicaID)
	p.InstanceId = uint64(*p.pb.InstanceID)
	if p.Ballot == nil {
		p.Ballot = new(Ballot)
	}
	p.Ballot.FromProtobuf(p.pb.Ballot)
	p.From = uint8(*p.pb.From)
	return nil
}

func (p *PrepareReply) Sender() uint8 {
	return p.From
}

func (p *PrepareReply) Type() uint8 {
	return PrepareReplyMsg
}

func (p *PrepareReply) Content() interface{} {
	return p
}

func (p *PrepareReply) Replica() uint8 {
	return p.ReplicaId
}

func (p *PrepareReply) Instance() uint64 {
	return p.InstanceId
}

func (p *PrepareReply) String() string {
	return fmt.Sprintf("PrepareReply, Instance[%v][%v], Ballot[%v], Original Ballot[%v]",
		p.ReplicaId, p.InstanceId, p.Ballot.String(), p.OriginalBallot.String())
}

func (p *PrepareReply) MarshalBinay() ([]byte, error) {
	replicaID := uint32(p.ReplicaId)
	instanceID := uint64(p.InstanceId)
	state := protobuf.State(p.Status)
	from := uint32(p.From)

	p.pb.ReplicaID = &replicaID
	p.pb.InstanceID = &instanceID
	p.pb.State = &state
	p.pb.Cmds = p.Cmds.ToBytesSlice()
	p.pb.Deps = p.Deps
	p.pb.Ballot = p.Ballot.ToProtobuf()
	p.pb.OriginalBallot = p.OriginalBallot.ToProtobuf()
	p.pb.IsFromLeader = &p.IsFromLeader
	p.pb.From = &from

	data, err := p.pb.Marshal()
	if err != nil {
		glog.Warning("PrepareReply: MarshalBinary() error: ", err)
		return nil, err
	}
	return data, nil
}

func (p *PrepareReply) UnmarshalBinary(data []byte) error {
	if err := p.pb.Unmarshal(data); err != nil {
		glog.Warning("PrepareReply: UnmarshalBinary() error: ", err)
		return err
	}

	p.ReplicaId = uint8(*p.pb.ReplicaID)
	p.InstanceId = uint64(*p.pb.InstanceID)
	p.Status = uint8(*p.pb.State)
	p.Cmds.FromBytesSlice(p.pb.Cmds)
	p.Deps = p.pb.Deps
	if p.Ballot == nil {
		p.Ballot = new(Ballot)
	}
	p.Ballot.FromProtobuf(p.pb.Ballot)
	if p.OriginalBallot == nil {
		p.OriginalBallot = new(Ballot)
	}
	p.OriginalBallot.FromProtobuf(p.pb.OriginalBallot)
	p.IsFromLeader = *p.pb.IsFromLeader
	p.From = uint8(*p.pb.From)
	return nil
}
