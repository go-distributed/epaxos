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
	param := &Param{
		ReplicaId:    3,
		Size:         5,
		StateMachine: new(test.DummySM),
	}
	r := New(param)

	assert.True(t, r.Id == 3)
	assert.True(t, r.Size == 5)
	assert.True(t, len(r.MaxInstanceNum) == 5)
	assert.True(t, len(r.InstanceMatrix) == 5)
	assert.Equal(t, r.StateMachine, new(test.DummySM))
	assert.True(t, r.Epoch == 1)

	for i := range r.InstanceMatrix {
		assert.True(t, len(r.InstanceMatrix[i]) == defaultInstancesLength)
	}

	param.Size = 4
	assert.Panics(t, func() { New(param) })
}

func TestMakeInitialBallot(t *testing.T) {
	param := &Param{
		ReplicaId:    3,
		Size:         5,
		StateMachine: new(test.DummySM),
	}
	r := New(param)
	r.Epoch = 3
	b := r.makeInitialBallot()
	assert.Equal(t, b, data.NewBallot(3, 0, 3))
}

// return a replica with id=5, size=5, and maxinstancenum of [1,2,3,4,5]
func depsTestSetupReplica() (r *Replica, i *Instance) {
	param := &Param{
		ReplicaId:    4,
		Size:         5,
		StateMachine: new(test.DummySM),
	}
	r = New(param)
	for i := 0; i < 5; i++ {
		r.MaxInstanceNum[i] = uint64(conflictNotFound + 1 + uint64(i))
		instance := NewInstance(r, r.Id, conflictNotFound+1+uint64(i))
		instance.cmds = commonTestlibExampleCommands().Clone()
		r.InstanceMatrix[i][instance.id] = instance
	}
	i = NewInstance(r, r.Id, 6)
	return
}

// If commands are conflicted with instance on each space [1, 2, 3, 4, 5].
// It should return deps=[1,2,3,4,5]
func TestInitInstance(t *testing.T) {
	r, i := depsTestSetupReplica()
	Cmds := commonTestlibExampleCommands()
	r.initInstance(Cmds, i)

	assert.Equal(t, i.cmds, Cmds)
	assert.Equal(t, i.deps, data.Dependencies{1, 2, 3, 4, 5})
}

// If no change in deps, it should return changed=false
// If changes in deps, it should return changed=true
func TestUpdateInstance(t *testing.T) {
	r, i := depsTestSetupReplica()
	cmds := commonTestlibExampleCommands()

	deps := data.Dependencies{1, 2, 3, 4, 5}

	changed := r.updateInstance(cmds, deps, r.Id, i)
	// won't search at all. so no changes.
	assert.False(t, changed)
	assert.Equal(t, i.deps, deps)

	emptyDeps := data.Dependencies{2, 0, 0, 0, 0}
	expectedDeps := data.Dependencies{2, 2, 3, 4, 5} // it's from r0

	changed = r.updateInstance(cmds, emptyDeps, 0, i)
	assert.True(t, changed)
	assert.Equal(t, i.deps, expectedDeps)
}
