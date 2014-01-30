package replica

import (
	"fmt"
	"testing"

	"github.com/go-epaxos/epaxos/data"
	"github.com/go-epaxos/epaxos/test"
	"github.com/stretchr/testify/assert"
)

var _ = fmt.Printf

func TestNewReplica(t *testing.T) {
	r := New(3, 5, new(test.DummySM))

	assert.True(t, r.Id == 3)
	assert.True(t, r.Size == 5)
	assert.True(t, len(r.MaxInstanceNum) == 5)
	assert.True(t, len(r.InstanceMatrix) == 5)
	assert.Equal(t, r.StateMachine, new(test.DummySM))
	assert.True(t, r.Epoch == 0)

	for i := range r.InstanceMatrix {
		assert.True(t, len(r.InstanceMatrix[i]) == defaultInstancesLength)
	}

	assert.Panics(t, func() { New(3, 4, new(test.DummySM)) })
}

func TestMakeInitialBallot(t *testing.T) {
	r := New(3, 5, new(test.DummySM))
	r.Epoch = 3
	b := r.makeInitialBallot()
	assert.Equal(t, b, data.NewBallot(3, 0, 3))
}

// return a replica with id=5, size=5, and maxinstancenum of [1,2,3,4,5]
func depsTestSetupReplica() (r *Replica) {
	r = New(5, 5, new(test.DummySM))
	for i := 0; i < 5; i++ {
		r.MaxInstanceNum[i] = uint64(conflictNotFound + 1 + uint64(i))
		instance := NewInstance(r, conflictNotFound+1+uint64(i))
		instance.cmds = commonTestlibExampleCommands().GetCopy()
		r.InstanceMatrix[i][instance.id] = instance
	}
	return
}

// If commands are conflicted with instance on each space [1, 2, 3, 4, 5].
// It should return seq=1, deps=[1,2,3,4,5]
func TestFindDependencies(t *testing.T) {
	r := depsTestSetupReplica()
	cmds := commonTestlibExampleCommands()
	seq, deps := r.findDependencies(cmds)

	assert.Equal(t, seq, uint32(1))
	assert.Equal(t, deps, data.Dependencies{1, 2, 3, 4, 5})
}

// If no change in deps, it should return changed=false and not change seq, deps
// If changes in deps, it should return changed=true and return updated seq, deps
func TestUpdateDependencies(t *testing.T) {
	r := depsTestSetupReplica()
	cmds := commonTestlibExampleCommands()

	seq := uint32(0)
	selfDeps := data.Dependencies{1, 2, 3, 4, 5}

	notChangedSeq, notChangedDeps, changed := r.updateDependencies(cmds, seq, selfDeps, 5)
	assert.False(t, changed)
	assert.Equal(t, notChangedSeq, seq)
	assert.Equal(t, notChangedDeps, selfDeps)

	emptyDeps := data.Dependencies{0, 0, 0, 0, 0}
	expectedDeps := data.Dependencies{0, 2, 3, 4, 5} // it's from r0

	changedSeq, changedDeps, changed := r.updateDependencies(
		cmds, 0, emptyDeps, 0)
	assert.True(t, changed)
	assert.Equal(t, changedSeq, uint32(1))
	assert.Equal(t, changedDeps, expectedDeps)
}
