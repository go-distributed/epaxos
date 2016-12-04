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

func (p *Prepare) MarshalProtobuf() ([]byte, error) {
	replicaID := uint32(p.ReplicaId)
	instanceID := uint64(p.InstanceId)
	from := uint32(p.From)

	p.pb.ReplicaID = &replicaID
	p.pb.InstanceID = &instanceID
	p.pb.Ballot = p.Ballot.ToProtobuf()
	p.pb.From = &from

	data, err := p.pb.Marshal()
	if err != nil {
		glog.Warning("Prepare: MarshalProtobuf() error: ", err)
		return nil, err
	}
	return data, nil
}

func (p *Prepare) UnmarshalProtobuf(data []byte) error {
	if err := p.pb.Unmarshal(data); err != nil {
		glog.Warning("Prepare: UnmarshalProtobuf() error: ", err)
		return err
	}

	p.ReplicaId = uint8(p.pb.GetReplicaID())
	p.InstanceId = uint64(p.pb.GetInstanceID())
	if p.Ballot == nil {
		p.Ballot = new(Ballot)
	}
	p.Ballot.FromProtobuf(p.pb.GetBallot())
	p.From = uint8(p.pb.GetFrom())
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

func (p *PrepareReply) MarshalProtobuf() ([]byte, error) {
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
		glog.Warning("PrepareReply: MarshalProtobuf() error: ", err)
		return nil, err
	}
	return data, nil
}

func (p *PrepareReply) UnmarshalProtobuf(data []byte) error {
	if err := p.pb.Unmarshal(data); err != nil {
		glog.Warning("PrepareReply: UnmarshalProtobuf() error: ", err)
		return err
	}

	p.ReplicaId = uint8(p.pb.GetReplicaID())
	p.InstanceId = uint64(p.pb.GetInstanceID())
	p.Status = uint8(p.pb.GetState())
	p.Cmds.FromBytesSlice(p.pb.GetCmds())
	p.Deps = p.pb.GetDeps()
	if p.Ballot == nil {
		p.Ballot = new(Ballot)
	}
	p.Ballot.FromProtobuf(p.pb.GetBallot())
	if p.OriginalBallot == nil {
		p.OriginalBallot = new(Ballot)
	}
	p.OriginalBallot.FromProtobuf(p.pb.GetOriginalBallot())
	p.IsFromLeader = p.pb.GetIsFromLeader()
	p.From = uint8(p.pb.GetFrom())
	return nil
}
