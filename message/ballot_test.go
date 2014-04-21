package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewAndInitialBallot(t *testing.T) {
	b := NewBallot(2, 3, 4)
	assert.Equal(t, b.Epoch, uint32(2))
	assert.Equal(t, b.Number, uint64(3))
	assert.Equal(t, b.ReplicaId, uint8(4))
}

func TestBallotToUint64(t *testing.T) {
	b := &Ballot{0, 0, 1}
	assert.Equal(t, b.ToUint64(), uint64(1))

	b = &Ballot{0, 1, 0}
	assert.Equal(t, b.ToUint64(), uint64(1<<ballotReplicaIdWidth))

	b = &Ballot{1, 0, 0}
	assert.Equal(t, b.ToUint64(), uint64(1<<(ballotReplicaIdWidth+ballotNumberWidth)))
}

func TestBallotFromUint64(t *testing.T) {
	b := NewBallot(2, 2, 2)
	assert.Equal(t, b.Epoch, uint32(2))
	assert.Equal(t, b.Number, uint64(2))
	assert.Equal(t, b.ReplicaId, uint8(2))

	b.FromUint64((1 << (ballotReplicaIdWidth + ballotNumberWidth)) | (1 << ballotReplicaIdWidth) | 1)
	assert.Equal(t, b.Epoch, uint32(1))
	assert.Equal(t, b.Number, uint64(1))
	assert.Equal(t, b.ReplicaId, uint8(1))
}

func TestBallotCompare(t *testing.T) {
	var b1, b2 *Ballot
	b1 = &Ballot{0, 0, 1}
	b2 = &Ballot{0, 0, 1}

	assert.True(t, b1.Compare(b2) == 0)

	b2 = &Ballot{0, 0, 2}
	assert.True(t, b1.Compare(b2) < 0)

	b2 = &Ballot{0, 0, 0}
	assert.True(t, b1.Compare(b2) > 0)

	b1 = &Ballot{0, 1, 0}
	b2 = &Ballot{0, 1, 0}
	assert.True(t, b1.Compare(b2) == 0)

	b2 = &Ballot{0, 2, 0}
	assert.True(t, b1.Compare(b2) < 0)

	b2 = &Ballot{0, 0, 0}
	assert.True(t, b1.Compare(b2) > 0)

	b1 = &Ballot{1, 0, 0}
	b2 = &Ballot{1, 0, 0}
	assert.True(t, b1.Compare(b2) == 0)

	b2 = &Ballot{2, 0, 0}
	assert.True(t, b1.Compare(b2) < 0)

	b2 = &Ballot{0, 0, 0}
	assert.True(t, b1.Compare(b2) > 0)

	b1 = &Ballot{1, 0, 0}
	b2 = &Ballot{0, 2, 0}
	assert.True(t, b1.Compare(b2) > 0)

	b2 = &Ballot{0, 0, 3}
	assert.True(t, b1.Compare(b2) > 0)

	b1 = &Ballot{0, 1, 0}
	assert.True(t, b1.Compare(b2) > 0)

	assert.Panics(t, func() { b1.Compare(nil) })
}

func TestIncNumber(t *testing.T) {
	b := NewBallot(2, 34, 4)
	b.IncNumber()
	assert.Equal(t, b.Epoch, uint32(2))
	assert.Equal(t, b.Number, uint64(35))
	assert.Equal(t, b.ReplicaId, uint8(4))
}

func TestSetReplicaId(t *testing.T) {
	b := NewBallot(2, 34, 4)
	assert.Equal(t, b.ReplicaId, uint8(4))

	b.SetReplicaId(6)
	assert.Equal(t, b.ReplicaId, uint8(6))
}

func TestIncNumClone(t *testing.T) {
	b := NewBallot(2, 34, 4)
	c := b.IncNumClone()
	assert.Equal(t, c.Number, uint64(35))
}

func TestBallotClone(t *testing.T) {
	b := NewBallot(2, 34, 4)
	c := b.Clone()

	assert.True(t, &b != &c)
	assert.Equal(t, b, c)

	var d *Ballot
	assert.Panics(t, func() { d.Clone() })
}

func TestBallotEpoch(t *testing.T) {
	b := NewBallot(2, 33, 4)
	assert.Equal(t, b.GetEpoch(), uint8(2))
}

func TestBallotIsInitialBallot(t *testing.T) {
	b := NewBallot(2, 0, 3)
	assert.True(t, b.IsInitialBallot())
}

func TestBallotSetNumber(t *testing.T) {
	b := NewBallot(1, 2, 3)
	b.SetNumber(4)
	assert.Equal(t, b.GetNumber(), uint64(4))
}
