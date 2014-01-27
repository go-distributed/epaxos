package replica

import (
	"fmt"
	"testing"

	"github.com/go-epaxos/epaxos/data"
	"github.com/go-epaxos/epaxos/test"
	"github.com/stretchr/testify/assert"
)

var _ = fmt.Printf

// **************************
// **** COMMON ROUTINE ******
// **************************

func commonTestlibExampleCommands() data.Commands {
	return data.Commands{
		data.Command("hello"),
	}
}

// ************************
// ****** Nil Status ******
// ************************

func instanceTestNilStatusProcessSetup() *Instance {
	r := New(0, 5, new(test.DummySM))
	i := NewInstance(r, conflictNotFound+1)
	return i
}

// If a nilstatus instance receives propose, it should change its status to
// preaccepted, return (broadcastAction, pre-accept message) and setup relevant
// information.
// The instance should also be ready to receive pre-accept reply. That means the
// relevant info should be set.
func TestNilStatusProcessPropose(t *testing.T) {
	// test panics
	inst := instanceTestNilStatusProcessSetup()
	inst.status = nilStatus
	inst.seq = 1
	assert.Panics(t, func() { inst.nilStatusProcess(&data.Propose{}) })

	i := instanceTestNilStatusProcessSetup()
	assert.Panics(t, func() { i.nilStatusProcess(&data.Propose{}) })

	i.status = nilStatus
	assert.Panics(t, func() { i.nilStatusProcess(&data.Prepare{}) })

	p := &data.Propose{
		Cmds: commonTestlibExampleCommands(),
	}
	action, m := i.nilStatusProcess(p)
	if !assert.IsType(t, &data.PreAccept{}, m) {
		t.Fatal("")
	}

	pa := m.(*data.PreAccept)
	assert.Equal(t, i.status, preAccepted)
	assert.Equal(t, action, fastQuorumAction)

	if !assert.ObjectsAreEqual(pa, &data.PreAccept{
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Cmds:       commonTestlibExampleCommands(),
		Seq:        0,
		Deps:       i.deps,
		Ballot:     i.replica.MakeInitialBallot(),
	}) {
		fmt.Printf("%v\n", pa)
		t.Fatal("")
	}

	assert.Equal(t, i.info.preAcceptCount, 0)
	assert.Equal(t, i.info.preAcceptNackCount, 0)
	assert.True(t, i.info.isFastPath)
}

func TestNilStatusProcessPreAccept(t *testing.T) {
}

func TestNilStatusProcessAccept(t *testing.T) {
}

func TestNilStatusProcessCommit(t *testing.T) {
}

func TestNilStatusOnCommitDependency(t *testing.T) {
}

// **********************
// *****  ACCEPTED ******
// **********************

func TestAcceptedProcessPrepare(t *testing.T) {
}

// **********************
// ***** COMMITTED ******
// **********************

// When a committed instance receives pre-accept reply, it should ignore it
func TestCommittedProcessPreAcceptReply(t *testing.T) {
	// create an new instance
	r := New(0, 5, new(test.DummySM))
	i := NewInstance(r, conflictNotFound+1)
	// set its status to committed
	i.status = committed
	// send a pre-accept message to it
	msg := &data.PreAcceptReply{}
	action, retMsg := i.committedProcess(msg)

	// expect:
	// - action: NoAction
	// - message: nil
	// - instance not changed
	assert.Equal(t, action, noAction, "")
	assert.Nil(t, retMsg, "")
}

// TestCheckStatus tests the behaviour of checkStatus
func TestCheckStatus(t *testing.T) {
	i := &Instance{
		status: committed,
	}

	assert.Panics(t, func() { i.checkStatus(preAccepted, accepted, preparing, executed) })
	assert.NotPanics(t, func() { i.checkStatus(committed) })
}
