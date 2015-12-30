package message

import (
	"fmt"

	"github.com/go-distributed/epaxos/protobuf"
)

const (
	// Ballot has a format like:
	// Epoch   | Number  | ReplicaId
	// 20 bits | 36 bits | 8 bits
	ballotEpochWidth     uint = 20
	ballotNumberWidth    uint = 36
	ballotReplicaIdWidth uint = 8

	ballotEpochMask     uint64 = ((1 << ballotEpochWidth) - 1) << (ballotNumberWidth + ballotReplicaIdWidth)
	ballotNumberMask    uint64 = ((1 << ballotNumberWidth) - 1) << (ballotReplicaIdWidth)
	ballotReplicaIdMask uint64 = (1 << ballotReplicaIdWidth) - 1
)

type Ballot struct {
	Epoch     uint32
	Number    uint64
	ReplicaId uint8
}

func NewBallot(epoch uint32, number uint64, replicId uint8) *Ballot {
	return &Ballot{
		Epoch:     epoch,
		Number:    number,
		ReplicaId: replicId,
	}
}

func (b *Ballot) ToUint64() uint64 {
	return ((uint64(b.Epoch) << (ballotNumberWidth + ballotReplicaIdWidth)) |
		(b.Number << ballotReplicaIdWidth) |
		uint64(b.ReplicaId))
}

func (b *Ballot) FromUint64(num uint64) {
	b.Epoch = uint32((num & ballotEpochMask) >> (ballotNumberWidth + ballotReplicaIdWidth))
	b.Number = ((num & ballotNumberMask) >> ballotReplicaIdWidth)
	b.ReplicaId = uint8(num & ballotReplicaIdMask)
}

func (b *Ballot) Compare(other *Ballot) int {
	if b == nil || other == nil {
		panic("Compare: ballot should not be nil")
	}
	if b.Epoch > other.Epoch {
		return 1
	}
	if b.Epoch < other.Epoch {
		return -1
	}
	if b.Number > other.Number {
		return 1
	}
	if b.Number < other.Number {
		return -1
	}
	if b.ReplicaId > other.ReplicaId {
		return 1
	}
	if b.ReplicaId < other.ReplicaId {
		return -1
	}

	return 0
}

func (b *Ballot) GetEpoch() uint32 {
	return b.Epoch

}

func (b *Ballot) GetNumber() uint64 {
	return b.Number
}

func (b *Ballot) SetNumber(number uint64) {
	b.Number = number
}

func (b *Ballot) GetReplicaId() uint8 {
	return b.ReplicaId
}

func (b *Ballot) IncNumber() {
	b.Number++
}

func (b *Ballot) SetReplicaId(rId uint8) {
	b.ReplicaId = uint8(rId)
}

func (b *Ballot) IncNumClone() *Ballot {
	return &Ballot{
		Epoch:     b.Epoch,
		Number:    b.Number + 1,
		ReplicaId: b.ReplicaId,
	}
}

func (b *Ballot) IsInitialBallot() bool {
	return b.Number == 0
}

func (b *Ballot) Clone() *Ballot {
	if b == nil {
		panic("")
	}
	return &Ballot{
		Epoch:     b.Epoch,
		Number:    b.Number,
		ReplicaId: b.ReplicaId,
	}
}

func (b *Ballot) String() string {
	return fmt.Sprintf("%v.%v.%v", b.Epoch, b.Number, b.ReplicaId)
}

func (b *Ballot) ToProtobuf() *protobuf.Ballot {
	epoch, number, replicaID := b.Epoch, b.Number, uint32(b.ReplicaId)
	return &protobuf.Ballot{
		Epoch:     &epoch,
		Number:    &number,
		ReplicaID: &replicaID,
	}
}

func (b *Ballot) FromProtobuf(pballot *protobuf.Ballot) {
	b.Epoch = uint32(*pballot.Epoch)
	b.Number = uint64(*pballot.Number)
	b.ReplicaId = uint8(*pballot.ReplicaID)
}
