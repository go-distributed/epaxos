package replica

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-distributed/epaxos"
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
	r, _ := New(param)

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
	_, err := New(param)
	assert.NotNil(t, err)
}

func TestMakeInitialBallot(t *testing.T) {
	param := &Param{
		ReplicaId:    3,
		Size:         5,
		StateMachine: new(test.DummySM),
	}
	r, _ := New(param)
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
	r, _ = New(param)
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

	// should panic since the instance is not committed
	assert.Panics(t, func() { r.resolveConflicts(r.InstanceMatrix[0][1]) })

	r.InstanceMatrix[0][1].status = committed
	assert.True(t, r.resolveConflicts(r.InstanceMatrix[0][1]))

	sccResultInstances := make([]*Instance, 0)
	for _, instances := range r.sccResults {
		sccResultInstances = append(sccResultInstances, instances...)
	}
	assert.Equal(t, len(sccResultInstances), 1)
	assert.Equal(t, sccResultInstances[0], r.InstanceMatrix[0][1])
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
	r.InstanceMatrix[0][3].status = committed

	assert.True(t, r.resolveConflicts(r.InstanceMatrix[0][3]))

	sccResultInstances := make([]*Instance, 0)
	for _, instances := range r.sccResults {
		sccResultInstances = append(sccResultInstances, instances...)
	}
	assert.Equal(t, len(sccResultInstances), 6)
	i := 0
	for i = range r.InstanceMatrix {
		assert.Equal(t, sccResultInstances[i], r.InstanceMatrix[i][i+2])
	}
	i++
	assert.Equal(t, sccResultInstances[i], r.InstanceMatrix[0][3])
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

	// test result list
	sccResultInstances := make([]*Instance, 0)
	for _, instances := range r.sccResults {
		sccResultInstances = append(sccResultInstances, instances...)
	}
	assert.Equal(t, len(sccResultInstances), 11)
	j := 0
	for i := range r.InstanceMatrix {
		assert.Equal(t, sccResultInstances[j], r.InstanceMatrix[i][i+2])
		j++
		assert.Equal(t, sccResultInstances[j], r.InstanceMatrix[i][i+4])
		j++
	}
	assert.Equal(t, sccResultInstances[j], r.InstanceMatrix[0][6])
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

	// test result list
	// scc components
	sccResultInstances := make([]*Instance, 0)
	for _, instances := range r.sccResults {
		sccResultInstances = append(sccResultInstances, instances...)
	}
	assert.Equal(t, len(sccResultInstances), 11)

	assert.Equal(t, sccResultInstances[0], r.InstanceMatrix[0][2])
	assert.Equal(t, sccResultInstances[1], r.InstanceMatrix[0][4])
	assert.Equal(t, sccResultInstances[2], r.InstanceMatrix[1][3])
	assert.Equal(t, sccResultInstances[3], r.InstanceMatrix[1][5])

	// other nodes
	assert.Equal(t, sccResultInstances[4], r.InstanceMatrix[2][4])
	assert.Equal(t, sccResultInstances[5], r.InstanceMatrix[2][6])
	assert.Equal(t, sccResultInstances[6], r.InstanceMatrix[3][5])
	assert.Equal(t, sccResultInstances[7], r.InstanceMatrix[3][7])
	assert.Equal(t, sccResultInstances[8], r.InstanceMatrix[4][6])
	assert.Equal(t, sccResultInstances[9], r.InstanceMatrix[4][8])

	// last node
	assert.Equal(t, sccResultInstances[10], r.InstanceMatrix[0][6])
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

	// test result list
	// scc components
	sccResultInstances := make([]*Instance, 0)
	for _, instances := range r.sccResults {
		sccResultInstances = append(sccResultInstances, instances...)
	}
	assert.Equal(t, len(sccResultInstances), 8)
	assert.Equal(t, sccResultInstances[0], r.InstanceMatrix[0][2])
	assert.Equal(t, sccResultInstances[1], r.InstanceMatrix[0][4])
	assert.Equal(t, sccResultInstances[2], r.InstanceMatrix[1][3])
	assert.Equal(t, sccResultInstances[3], r.InstanceMatrix[1][5])

	// other nodes
	assert.Equal(t, sccResultInstances[4], r.InstanceMatrix[2][4])
	assert.Equal(t, sccResultInstances[5], r.InstanceMatrix[2][6])
	assert.Equal(t, sccResultInstances[6], r.InstanceMatrix[3][5])
	assert.Equal(t, sccResultInstances[7], r.InstanceMatrix[3][7])
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

	// test result list
	// scc components
	sccResultInstances := make([]*Instance, 0)
	for _, instances := range r.sccResults {
		sccResultInstances = append(sccResultInstances, instances...)
	}
	assert.Equal(t, len(sccResultInstances), 9)
	assert.Equal(t, sccResultInstances[0], r.InstanceMatrix[0][2])
	assert.Equal(t, sccResultInstances[1], r.InstanceMatrix[0][4])
	assert.Equal(t, sccResultInstances[2], r.InstanceMatrix[1][3])
	assert.Equal(t, sccResultInstances[3], r.InstanceMatrix[1][5])

	// other nodes
	assert.Equal(t, sccResultInstances[4], r.InstanceMatrix[2][4])
	assert.Equal(t, sccResultInstances[5], r.InstanceMatrix[2][6])
	assert.Equal(t, sccResultInstances[6], r.InstanceMatrix[3][5])
	assert.Equal(t, sccResultInstances[7], r.InstanceMatrix[3][7])

	// last nodes
	assert.Equal(t, sccResultInstances[8], r.InstanceMatrix[0][6])
}

// a helper to make committed instances, containing scc, no un-committed, nor executed instances
func makeCommitedInstances(r *Replica) {
	r.InstanceMatrix[0][6] = NewInstance(r, 0, 6)
	r.InstanceMatrix[0][6].cmds = data.Commands{
		data.Command("[0][6]"),
		data.Command("[0][6]"),
	}
	r.InstanceMatrix[0][6].status = committed
	r.InstanceMatrix[0][6].deps = data.Dependencies{4, 5, 6, 7, 8}

	// create 1st level deps (4, 5, 6, 7, 8)
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+4] = NewInstance(r, uint8(i), uint64(i+4))
		r.InstanceMatrix[i][i+4].status = committed
	}
	r.InstanceMatrix[0][4].deps = data.Dependencies{2, 0, 0, 0, 0}
	r.InstanceMatrix[0][4].cmds = data.Commands{
		data.Command("[0][4]"),
		data.Command("[0][4]"),
	}

	r.InstanceMatrix[1][5].deps = data.Dependencies{0, 3, 0, 0, 0}
	r.InstanceMatrix[1][5].cmds = data.Commands{
		data.Command("[1][5]"),
		data.Command("[1][5]"),
	}

	r.InstanceMatrix[2][6].deps = data.Dependencies{0, 0, 4, 0, 0}
	r.InstanceMatrix[2][6].cmds = data.Commands{
		data.Command("[2][6]"),
		data.Command("[2][6]"),
	}

	r.InstanceMatrix[3][7].deps = data.Dependencies{0, 0, 0, 5, 0}
	r.InstanceMatrix[3][7].cmds = data.Commands{
		data.Command("[3][7]"),
		data.Command("[3][7]"),
	}

	r.InstanceMatrix[4][8].deps = data.Dependencies{0, 0, 0, 0, 6}
	r.InstanceMatrix[4][8].cmds = data.Commands{
		data.Command("[4][8]"),
		data.Command("[4][8]"),
	}

	// create 2nd level deps (2, 3, 4, 5, 6)
	for i := range r.InstanceMatrix {
		r.InstanceMatrix[i][i+2] = NewInstance(r, uint8(i), uint64(i+2))
		r.InstanceMatrix[i][i+2].status = committed
		r.InstanceMatrix[i][i+2].cmds = data.Commands{
			data.Command(fmt.Sprintf("[%d][%d]", i, i+2)),
			data.Command(fmt.Sprintf("[%d][%d]", i, i+2)),
		}
	}

	// create a scc (2->4, 2->5, 3->4, 3->5)
	r.InstanceMatrix[0][2].deps = data.Dependencies{4, 5, 0, 0, 0}
	r.InstanceMatrix[1][3].deps = data.Dependencies{4, 5, 0, 0, 0}
}

// This func tests the result of executeList()
func TestExecuteList(t *testing.T) {
	r := commonTestlibExampleReplica()
	r.StateMachine = test.NewDummySM()

	makeCommitedInstances(r)
	// resolve conflicts
	assert.True(t, r.resolveConflicts(r.InstanceMatrix[0][6]))
	assert.Nil(t, r.executeList())

	// construct executionLog
	expectLogStr := "["
	expectLogStr += "[0][2] [0][2] "
	expectLogStr += "[0][4] [0][4] "
	expectLogStr += "[1][3] [1][3] "
	expectLogStr += "[1][5] [1][5] "

	for i := 2; i < int(r.Size); i++ {
		expectLogStr += fmt.Sprintf("[%d][%d] [%d][%d] ", i, i+2, i, i+2)
		expectLogStr += fmt.Sprintf("[%d][%d] [%d][%d] ", i, i+4, i, i+4)
	}
	// replace the last white-space to `]`
	expectLogStr += "[0][6] [0][6]]"

	// test the execution result
	executionLogStr := fmt.Sprint(r.StateMachine.(*test.DummySM).ExecutionLog)
	assert.Equal(t, executionLogStr, expectLogStr)
}

// This func tests if executeList() will return error when there is
// an error returned by the state machine
func TestExecuteListWithError(t *testing.T) {
	r := commonTestlibExampleReplica()
	r.StateMachine = test.NewDummySM()
	makeCommitedInstances(r)

	// create an error
	r.InstanceMatrix[0][6].cmds = data.Commands{
		data.Command("error"),
	}

	// resolve conflicts
	assert.True(t, r.resolveConflicts(r.InstanceMatrix[0][6]))

	// should return an error
	assert.Equal(t, r.executeList(), epaxos.ErrStateMachineExecution)

	// construct executionLog
	expectLogStr := "["
	expectLogStr += "[0][2] [0][2] "
	expectLogStr += "[0][4] [0][4] "
	expectLogStr += "[1][3] [1][3] "
	expectLogStr += "[1][5] [1][5] "
	for i := 2; i < int(r.Size); i++ {
		expectLogStr += fmt.Sprintf("[%d][%d] [%d][%d] ", i, i+2, i, i+2)
		expectLogStr += fmt.Sprintf("[%d][%d] [%d][%d] ", i, i+4, i, i+4)
	}
	expectLogStr = expectLogStr[:len(expectLogStr)-1]
	expectLogStr += "]"

	// test the exection result
	executionLogStr := fmt.Sprint(r.StateMachine.(*test.DummySM).ExecutionLog)
	assert.Equal(t, executionLogStr, expectLogStr)
}

func makeTimeoutInstances(r *Replica) {
}

// This function tests the timeout mechanism
func TestNoTimeout1(t *testing.T) {
	r := commonTestlibExampleReplica()
	time.Sleep(2 * r.TimeoutInterval)
	go r.checkTimeout()
	select {
	case <-r.MessageEventChan:
		t.Fatal("shouldn't get a timeout message")
	default:
	}
}

// Should not timeout for a committed instance
func TestNoTimeout2(t *testing.T) {
	r := commonTestlibExampleReplica()
	r.InstanceMatrix[0][1] = commonTestlibExampleCommittedInstance()
	r.MaxInstanceNum[0] = 1
	time.Sleep(2 * r.TimeoutInterval)
	go r.checkTimeout()
	select {
	case <-r.MessageEventChan:
		t.Fatal("shouldn't get a timeout message for committed instance")
	default:
	}
}

// test one timeout
func TestTimeout1(t *testing.T) {
	r := commonTestlibExampleReplica()
	r.InstanceMatrix[0][1] = commonTestlibExampleAcceptedInstance()
	r.MaxInstanceNum[0] = 1
	time.Sleep(2 * r.TimeoutInterval)
	go r.checkTimeout()
	time.Sleep(r.TimeoutInterval) // wait for message sending

	select {
	case <-r.MessageEventChan:
	default:
		t.Fatal("should get a timeout message from a uncommitted instance")
	}

	time.Sleep(r.TimeoutInterval) // wait for message sending
	select {
	case <-r.MessageEventChan:
		t.Fatal("should get only one timeout message from a uncommitted instance")
	default:
	}
}

// test multiple timeouts
func TestTimeout2(t *testing.T) {
	r := commonTestlibExampleReplica()
	for i, inst := range r.InstanceMatrix {
		inst[1023] = commonTestlibExampleAcceptedInstance()
		inst[1025] = commonTestlibExampleAcceptedInstance()
		inst[1026] = commonTestlibExampleCommittedInstance()
		inst[1027] = commonTestlibExampleAcceptedInstance()
		r.MaxInstanceNum[i] = 1027
		r.ExecutedUpTo[i] = 1022
	}
	time.Sleep(2 * r.TimeoutInterval)
	go r.checkTimeout()

	// should receive 10 timeout message in total
	for i := 0; i < int(r.Size); i++ {
		for j := 1023; j <= 1027; j++ { // include a checkpoint
			time.Sleep(r.TimeoutInterval) // wait for message sending
			if j%2 == 0 {
				continue
			}
			select {
			case msg := <-r.MessageEventChan:
				assert.Equal(t, msg, &MessageEvent{
					From: r.Id,
					Message: &data.Timeout{
						ReplicaId:  uint8(i),
						InstanceId: uint64(j),
					},
				})
			default:
				t.Fatal("should get timeout messages")
			}
		}
	}
	time.Sleep(r.TimeoutInterval) // wait for message sending
	select {
	case <-r.MessageEventChan:
		t.Fatal("shouldn't get more timeout messages")
	default:
	}
}

// test the correctness of the propose id without batching
func TestProposeIdNoBatch(t *testing.T) {
	N := 5000

	param := &Param{
		ReplicaId:      0,
		Size:           5,
		StateMachine:   new(test.DummySM),
		EnableBatching: false,
	}
	r, _ := New(param)

	// only start the propose
	go r.proposeLoop()
	go r.eventLoop()
	defer close(r.stop)

	resultIDs := make([]chan uint64, N)
	expectIDs := make([]uint64, N)

	j := uint64(0)
	for i := 0; i < N; i++ {
		if r.IsCheckpoint(j) {
			j++
		}
		resultIDs[i] = r.Propose(data.Command("hello"))
		expectIDs[i] = j
		j++
	}

	for i := 0; i < N; i++ {
		assert.Equal(t, <-resultIDs[i], expectIDs[i])
	}
}

// test the correctness of the propose id with batching
func TestProposeIdWithBatch(t *testing.T) {
	N := 5000
	B := 100

	param := &Param{
		ReplicaId:      0,
		Size:           5,
		StateMachine:   new(test.DummySM),
		EnableBatching: true,
		BatchInterval:  time.Millisecond * 50,
	}
	r, _ := New(param)

	// only start the propose
	go r.proposeLoop()
	go r.eventLoop()
	defer close(r.stop)

	resultIDs := make([]chan uint64, N)
	expectIDs := make([]uint64, N)

	// let's batch 100 commands in a group
	exp := uint64(0)
	for i := 0; i < N/B; i++ {
		for j := 0; j < B; j++ {
			if r.IsCheckpoint(exp) {
				exp++
			}
			resultIDs[i*B+j] = r.Propose(data.Command("hello"))
			expectIDs[i*B+j] = exp
		}
		time.Sleep(param.BatchInterval * 2)
		exp++
	}

	for i := 0; i < N; i++ {
		assert.Equal(t, <-resultIDs[i], expectIDs[i])
	}
}
