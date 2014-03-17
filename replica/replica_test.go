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

// This func tests the correctness of inSccStack() pushSccStack() and  popSccStack()
func TestSccStack(t *testing.T) {
	i := commonTestlibExampleCommittedInstance()
	iClone := commonTestlibCloneInstance(i)
	iClone.ballot = iClone.ballot.IncNumClone()
	r := i.replica

	// push two items
	r.pushSccStack(i)
	r.pushSccStack(iClone)

	// test if they are valid in the stack
	assert.True(t, r.inSccStack(i))
	assert.True(t, r.inSccStack(iClone))

	// pop out one item and test if it's valid
	iPop := r.popSccStack()
	assert.Equal(t, iClone, iPop)
	assert.NotEqual(t, i, iPop)

	// test the content in the stack
	assert.False(t, r.inSccStack(iClone))
	assert.True(t, r.inSccStack(i))

	// pop out another one and test if it's valid
	iPop = r.popSccStack()
	assert.Equal(t, i, iPop)
	assert.NotEqual(t, iClone, iPop)

	// test the content of the stack
	assert.False(t, r.inSccStack(i))
}

// Testings to test the correctness of resolveConflicts()

// **********************
// ***** 1, no deps *****
// **********************
func TestResolveConflictsWithNoDeps(t *testing.T) {
	r := commonTestlibExampleReplica()
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][1] = NewInstance(r, uint8(i), 1)
	}

	assert.True(t, r.resolveConflicts(r.InstanceMatrix[0][1]))
	assert.Equal(t, r.sccResult.Len(), 1)
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][1])
}

// **************************************
// ***** 2, one level depth of deps *****
// **************************************
func TestResolveConflictsWithSimpleDeps(t *testing.T) {
	r := commonTestlibExampleReplica()
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+2] = NewInstance(r, uint8(i), uint64(i+2))
		r.InstanceMatrix[i][i+2].status = committed
	}
	r.InstanceMatrix[0][3] = NewInstance(r, 0, 3)
	r.InstanceMatrix[0][3].deps = data.Dependencies{2, 3, 4, 5, 6}
	assert.True(t, r.resolveConflicts(r.InstanceMatrix[0][3]))
	assert.Equal(t, r.sccResult.Len(), 6)

	for i := range r.InstanceMatrix {
		assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[i][i+2])
	}
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][3])
}

// ***************************************************************
// ***** 3, mutiple levels of depth of deps, (acyclic graph) *****
// ***************************************************************

// InstanceMatrix:
//
//   2 <---|         |---4 <------ 6
//   3 <---|         |---5 <-----/
//   4 <---| mapping |---6 <----/
//   5 <---|         |---7 <---/
//   6 <---|         |---8 <--/
//
func TestResolveConflictsWithMultipleLevelDeps(t *testing.T) {
	r := commonTestlibExampleReplica()
	r.InstanceMatrix[0][6] = NewInstance(r, 0, 6)
	r.InstanceMatrix[0][6].status = committed
	r.InstanceMatrix[0][6].deps = data.Dependencies{4, 5, 6, 7, 8}

	// create 1st level deps (4, 5, 6, 7, 8)
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+4] = NewInstance(r, uint8(i), uint64(i+4))
		r.InstanceMatrix[i][i+4].status = committed
	}
	r.InstanceMatrix[0][4].deps = data.Dependencies{2, 0, 0, 0, 0}
	r.InstanceMatrix[1][5].deps = data.Dependencies{2, 3, 0, 0, 0}
	r.InstanceMatrix[2][6].deps = data.Dependencies{2, 3, 4, 0, 0}
	r.InstanceMatrix[3][7].deps = data.Dependencies{2, 3, 4, 5, 0}
	r.InstanceMatrix[4][8].deps = data.Dependencies{2, 3, 4, 5, 6}

	// create 2nd level deps (2, 3, 4, 5, 6)
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+2] = NewInstance(r, uint8(i), uint64(i+2))
		r.InstanceMatrix[i][i+2].status = committed
	}

	assert.True(t, r.resolveConflicts(r.InstanceMatrix[0][6]))
	assert.Equal(t, r.sccResult.Len(), 11)

	// test result list
	for i := range r.InstanceMatrix {
		assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[i][i+2])
		assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[i][i+4])
	}
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][6])
}

// ******************************************************************
// ***** 4, multiple levels of depth of deps, (contains cycles) *****
// ******************************************************************

// InstanceMatrix:
//
//   --------------------------
//  |  |                       |
//  |  |                       |
//  |  2 <---|         |---4 <-|------- 6
//   --3 <---|         |---5 <-/-------/
//     4 <---| mapping |---6 <--------/
//     5 <---|         |---7 <-------/
//     6 <---|         |---8 <------/
//
func TestResolveConflictsWithSccDeps(t *testing.T) {
	r := commonTestlibExampleReplica()
	r.InstanceMatrix[0][6] = NewInstance(r, 0, 6)
	r.InstanceMatrix[0][6].status = committed
	r.InstanceMatrix[0][6].deps = data.Dependencies{4, 5, 6, 7, 8}

	// create 1st level deps (4, 5, 6, 7, 8)
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+4] = NewInstance(r, uint8(i), uint64(i+4))
		r.InstanceMatrix[i][i+4].status = committed
	}
	r.InstanceMatrix[0][4].deps = data.Dependencies{2, 0, 0, 0, 0}
	r.InstanceMatrix[1][5].deps = data.Dependencies{0, 3, 0, 0, 0}
	r.InstanceMatrix[2][6].deps = data.Dependencies{0, 0, 4, 0, 0}
	r.InstanceMatrix[3][7].deps = data.Dependencies{0, 0, 0, 5, 0}
	r.InstanceMatrix[4][8].deps = data.Dependencies{0, 0, 0, 0, 6}

	// create 2nd level deps (2, 3, 4, 5, 6)
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+2] = NewInstance(r, uint8(i), uint64(i+2))
		r.InstanceMatrix[i][i+2].status = committed
	}

	// create a scc (2->4, 2->5, 3->4, 3->5)
	r.InstanceMatrix[0][2].deps = data.Dependencies{4, 5, 0, 0, 0}
	r.InstanceMatrix[1][3].deps = data.Dependencies{4, 5, 0, 0, 0}

	assert.True(t, r.resolveConflicts(r.InstanceMatrix[0][6]))
	assert.Equal(t, r.sccResult.Len(), 11)

	// test result list
	// scc components
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[1][3])
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[1][5])
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][2])
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][4])

	// other nodes
	for i := 2; i < int(r.Size); i++ {
		assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[i][i+2])
		assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[i][i+4])
	}
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][6])
}

// ***************************************************************************************
// ***** 5, same multiple levels of depth of deps, but with an un-committed instance *****
// ***************************************************************************************
func TestResolveConflictsWithSccDepsAndUncommitedInstance(t *testing.T) {
	r := commonTestlibExampleReplica()
	r.InstanceMatrix[0][6] = NewInstance(r, 0, 6)
	r.InstanceMatrix[0][6].status = committed
	r.InstanceMatrix[0][6].deps = data.Dependencies{4, 5, 6, 7, 8}

	// create 1st level deps (4, 5, 6, 7, 8)
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+4] = NewInstance(r, uint8(i), uint64(i+4))
		r.InstanceMatrix[i][i+4].status = committed
	}
	r.InstanceMatrix[0][4].deps = data.Dependencies{2, 0, 0, 0, 0}
	r.InstanceMatrix[1][5].deps = data.Dependencies{0, 3, 0, 0, 0}
	r.InstanceMatrix[2][6].deps = data.Dependencies{0, 0, 4, 0, 0}
	r.InstanceMatrix[3][7].deps = data.Dependencies{0, 0, 0, 5, 0}
	r.InstanceMatrix[4][8].deps = data.Dependencies{0, 0, 0, 0, 6}

	// create 2nd level deps (2, 3, 4, 5, 6)
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+2] = NewInstance(r, uint8(i), uint64(i+2))
		r.InstanceMatrix[i][i+2].status = committed
	}

	// create a scc (2->4, 2->5, 3->4, 3->5)
	r.InstanceMatrix[0][2].deps = data.Dependencies{4, 5, 0, 0, 0}
	r.InstanceMatrix[1][3].deps = data.Dependencies{4, 5, 0, 0, 0}

	// create an un-committed instance
	r.InstanceMatrix[4][6].status = accepted

	assert.False(t, r.resolveConflicts(r.InstanceMatrix[0][6]))
	assert.Equal(t, r.sccResult.Len(), 8)

	// test result list
	// scc components
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[1][3])
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[1][5])
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][2])
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][4])

	// other nodes
	for i := 2; i < int(r.Size)-1; i++ {
		assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[i][i+2])
		assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[i][i+4])
	}
}

// ************************************************************************************
// ***** 6, same multiple levels of depth of deps, but with an executed instance ******
// ************************************************************************************
func TestResolveConflictsWithSccDepsAndexecutedInstance(t *testing.T) {
	r := commonTestlibExampleReplica()
	r.InstanceMatrix[0][6] = NewInstance(r, 0, 6)
	r.InstanceMatrix[0][6].status = committed
	r.InstanceMatrix[0][6].deps = data.Dependencies{4, 5, 6, 7, 8}

	// create 1st level deps (4, 5, 6, 7, 8)
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+4] = NewInstance(r, uint8(i), uint64(i+4))
		r.InstanceMatrix[i][i+4].status = committed
	}
	r.InstanceMatrix[0][4].deps = data.Dependencies{2, 0, 0, 0, 0}
	r.InstanceMatrix[1][5].deps = data.Dependencies{0, 3, 0, 0, 0}
	r.InstanceMatrix[2][6].deps = data.Dependencies{0, 0, 4, 0, 0}
	r.InstanceMatrix[3][7].deps = data.Dependencies{0, 0, 0, 5, 0}
	r.InstanceMatrix[4][8].deps = data.Dependencies{0, 0, 0, 0, 6}

	// create 2nd level deps (2, 3, 4, 5, 6)
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+2] = NewInstance(r, uint8(i), uint64(i+2))
		r.InstanceMatrix[i][i+2].status = committed
	}

	// create a scc (2->4, 2->5, 3->4, 3->5)
	r.InstanceMatrix[0][2].deps = data.Dependencies{4, 5, 0, 0, 0}
	r.InstanceMatrix[1][3].deps = data.Dependencies{4, 5, 0, 0, 0}

	// create an executed instance
	// [*] Note: The deps of [4][8] won't be executed either.
	r.InstanceMatrix[4][8].SetExecuted()

	assert.True(t, r.resolveConflicts(r.InstanceMatrix[0][6]))
	assert.Equal(t, r.sccResult.Len(), 9)

	// test result list
	// scc components
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[1][3])
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[1][5])
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][2])
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][4])

	// other nodes
	for i := 2; i < int(r.Size)-1; i++ {
		assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[i][i+2])
		assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[i][i+4])
	}
	assert.Equal(t, r.sccResult.Remove(r.sccResult.Front()), r.InstanceMatrix[0][6])
}
