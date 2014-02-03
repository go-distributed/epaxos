package replica

import (
	"fmt"
	"testing"

	"github.com/go-distributed/epaxos/data"
	"github.com/go-distributed/epaxos/test"
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
	assert.True(t, r.Epoch == 1)

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
func depsTestSetupReplica() (r *Replica, i *Instance) {
	r = New(5, 5, new(test.DummySM))
	for i := 0; i < 5; i++ {
		r.MaxInstanceNum[i] = uint64(conflictNotFound + 1 + uint64(i))
		instance := NewInstance(r, conflictNotFound+1+uint64(i))
		instance.cmds = commonTestlibExampleCommands().GetCopy()
		r.InstanceMatrix[i][instance.id] = instance
	}
	i = NewInstance(r, 6)
	return
}

// If commands are conflicted with instance on each space [1, 2, 3, 4, 5].
// It should return seq=1, deps=[1,2,3,4,5]
func TestInitInstance(t *testing.T) {
	r, i := depsTestSetupReplica()
	Cmds := commonTestlibExampleCommands()
	r.initInstance(Cmds, i)

	assert.Equal(t, i.cmds, Cmds)
	assert.Equal(t, i.seq, uint32(1))
	assert.Equal(t, i.deps, data.Dependencies{1, 2, 3, 4, 5})
}

// If no change in deps, it should return changed=false
// If changes in deps, it should return changed=true
func TestUpdateInstance(t *testing.T) {
	r, i := depsTestSetupReplica()
	cmds := commonTestlibExampleCommands()

	deps := data.Dependencies{1, 2, 3, 4, 5}

	changed := r.updateInstance(cmds, 0, deps, 5, i)
	assert.False(t, changed)
	// won't search at all. so seq isn't incremented.
	assert.Equal(t, i.seq, uint32(0))
	assert.Equal(t, i.deps, deps)

	emptyDeps := data.Dependencies{2, 0, 0, 0, 0}
	expectedDeps := data.Dependencies{2, 2, 3, 4, 5} // it's from r0

	changed = r.updateInstance(cmds, 0, emptyDeps, 0, i)
	assert.True(t, changed)
	assert.Equal(t, i.seq, uint32(1))
	assert.Equal(t, i.deps, expectedDeps)
}
