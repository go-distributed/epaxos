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
	p := &data.Propose{
		Cmds: commonTestlibExampleCommands(),
	}

	inst := instanceTestNilStatusProcessSetup()
	inst.status = nilStatus
	inst.seq = 1
	// test panics not freshly created nilStatus instance
	assert.Panics(t, func() { inst.nilStatusProcess(p) })

	// test panics instance's status is not nilStatus
	i := instanceTestNilStatusProcessSetup()
	assert.Panics(t, func() { i.nilStatusProcess(p) })

	i.status = nilStatus

	// test panics empty propose
	assert.Panics(t, func() { i.nilStatusProcess(&data.Propose{}) })

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
	pa := &data.PreAcceptReply{}
	action, m := i.committedProcess(pa)

	// expect:
	// - action: NoAction
	// - message: nil
	// - instance not changed
	assert.Equal(t, action, noAction, "")
	assert.Nil(t, m, "")
}

// TestCheckStatus tests the behaviour of checkStatus,
// on success: it panics when instance is not at those status,
// on failure: otherwise.
func TestCheckStatus(t *testing.T) {
	i := &Instance{
		status: committed,
	}

	assert.Panics(t, func() { i.checkStatus(preAccepted, accepted, preparing, executed) })
	assert.NotPanics(t, func() { i.checkStatus(committed) })
}

// TestRejections tests whether rejection functions work correctly,
// on success: we will get a bunch of replies with the field `Ok' == false,
// `Ballot', `ReplicaId' and `InstanceId' should be the same of the instance
// on failure: otherwise.
func TestRejections(t *testing.T) {
	inst := instanceTestNilStatusProcessSetup()
	inst.ballot = data.NewBallot(1, 2, inst.replica.Id)

	// reject PreAcceptReply
	action, msg := inst.rejectPreAccept()
	pa := msg.(*data.PreAcceptReply)
	assert.Equal(t, action, replyAction)
	assert.Equal(t, pa.Ok, false)
	assert.Equal(t, pa.ReplicaId, inst.replica.Id)
	assert.Equal(t, pa.InstanceId, inst.id)
	assert.Equal(t, pa.Ballot, inst.ballot)
	assert.True(t, &pa.Ballot != &inst.ballot)

	// reject AcceptReply
	action, msg = inst.rejectAccept()
	ar := msg.(*data.AcceptReply)
	assert.Equal(t, action, replyAction)
	assert.Equal(t, ar.Ok, false)
	assert.Equal(t, ar.ReplicaId, inst.replica.Id)
	assert.Equal(t, ar.InstanceId, inst.id)
	assert.Equal(t, ar.Ballot, inst.ballot)
	assert.True(t, &ar.Ballot != &inst.ballot)

	// reject PrepareReply
	action, msg = inst.rejectPrepare()
	pr := msg.(*data.PrepareReply)
	assert.Equal(t, action, replyAction)
	assert.Equal(t, pr.Ok, false)
	assert.Equal(t, pr.ReplicaId, inst.replica.Id)
	assert.Equal(t, pr.InstanceId, inst.id)
	assert.Equal(t, pr.Ballot, inst.ballot)
	assert.True(t, &pr.Ballot != &inst.ballot)
}

// TestHandlePrepare tests whether we handle Prepares correctly.
// on success: we will get a PrepareReply with appropriate fields
// on failure: otherwise
func TestHandlePrepare(t *testing.T) {
	inst := instanceTestNilStatusProcessSetup()
	ballot := data.NewBallot(1, 2, inst.replica.Id)
	inst.ballot = ballot.GetCopy()
	inst.status = committed
	deps := data.Dependencies{
		3,
		4,
		5,
		6,
		7,
	}
	inst.deps = deps.GetCopy()

	// NeedCmdsInReply == false
	largerBallot := data.NewBallot(2, 3, 3)
	prepare := &data.Prepare{
		ReplicaId:       3,
		InstanceId:      4,
		Ballot:          largerBallot,
		NeedCmdsInReply: false,
	}

	action, msg := inst.handlePrepare(prepare)
	pr := msg.(*data.PrepareReply)
	assert.Equal(t, action, replyAction)

	// test the reply
	assert.Equal(t, pr, &data.PrepareReply{
		Ok:         true,
		ReplicaId:  0,
		InstanceId: 1,
		Status:     committed,
		Cmds:       data.Commands(nil),
		Deps:       deps,
		Ballot:     prepare.Ballot,
	})

	assert.True(t, &pr.Deps != &inst.deps)
	assert.True(t, &pr.Ballot != &inst.ballot)

	assert.Equal(t, inst.ballot, prepare.Ballot)
	assert.True(t, &inst.ballot != &prepare.Ballot)

	// NeedCmdsInReply == true
	prepare = &data.Prepare{
		ReplicaId:       3,
		InstanceId:      4,
		Ballot:          data.NewBallot(2, 3, 3),
		NeedCmdsInReply: true,
	}
	cmds := data.Commands{
		data.Command("hello"),
		data.Command("world"),
	}
	inst.cmds = cmds.GetCopy()

	action, msg = inst.handlePrepare(prepare)
	pr = msg.(*data.PrepareReply)
	assert.Equal(t, action, replyAction)

	// test the reply
	assert.Equal(t, pr, &data.PrepareReply{
		Ok:         true,
		ReplicaId:  0,
		InstanceId: 1,
		Status:     committed,
		Cmds:       cmds,
		Deps:       deps,
		Ballot:     prepare.Ballot,
	})

	assert.True(t, &pr.Deps != &inst.deps)
	assert.True(t, &pr.Ballot != &inst.ballot)
	assert.True(t, &pr.Cmds != &inst.cmds)

	assert.Equal(t, inst.ballot, prepare.Ballot)
	assert.True(t, &inst.ballot != &prepare.Ballot)
}

// TestHandleCommit tests the functionality of handleCommit
// on success: handleCommit returns a no act and nil message,
// besides, the instances' status is set to commited.
// on failure: otherwise
func TestHandleCommit(t *testing.T) {
}
