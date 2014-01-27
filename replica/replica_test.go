package replica

import (
	"testing"

	"github.com/go-epaxos/epaxos/data"
	"github.com/go-epaxos/epaxos/test"
	"github.com/stretchr/testify/assert"
)

func TestNewReplica(t *testing.T) {
	r := New(3, 5, new(test.DummySM))

	assert.True(t, r.Id == 3)
	assert.True(t, r.Size == 5)
	assert.True(t, len(r.MaxInstanceNum) == 5)
	assert.True(t, len(r.InstanceMatrix) == 5)
	assert.Equal(t, r.StateMachine, new(test.DummySM))
	assert.True(t, r.Epoch == 0)

	for i := range r.InstanceMatrix {
		assert.True(t, len(r.InstanceMatrix[i]) == defaultInstanceNum)
	}

	assert.Panics(t, func() { New(3, 4, new(test.DummySM)) })
}

func TestMakeInitialBallot(t *testing.T) {
	r := New(3, 5, new(test.DummySM))
	r.Epoch = 3
	b := r.MakeInitialBallot()
	assert.Equal(t, b, data.NewBallot(3, 0, 3))
}

// TestFindeDependencies test whether findDependencies() works correctly,
// On success: it should return seq = 3, deps = {0, 0, 0, 4, 0}.
// On failure: otherwise
func TestFindDependencies(t *testing.T) {
	r1 := New(3, 5, new(test.DummySM))
	r3 := New(3, 5, new(test.DummySM))

	inst := NewInstance(r3, 3)
	inst.cmds = data.Commands{
		data.Command("hello"),
	}
	r1.InstanceMatrix[3][3] = inst

	inst = NewInstance(r3, 4)
	inst.cmds = data.Commands{
		data.Command("hello"),
	}
	inst.seq = 3
	r1.InstanceMatrix[3][4] = inst

	inst = NewInstance(r1, 5)
	inst.cmds = data.Commands{
		data.Command("world"),
	}
	r1.InstanceMatrix[1][4] = inst

	cmds := data.Commands{
		data.Command("hi"),
		data.Command("hello"),
	}

	r1.MaxInstanceNum = []uint64{10, 10, 10, 10, 10}
	seq, deps := r1.findDependencies(cmds)

	assert.Equal(t, seq, uint32(4))
	assert.Equal(t, deps, data.Dependencies{0, 0, 0, 4, 0})
}
