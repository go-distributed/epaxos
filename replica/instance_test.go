package replica

import (
	"fmt"
	"testing"

	"github.com/go-distributed/epaxos/data"
	"github.com/go-distributed/epaxos/test"
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

func commonTestlibExampleDeps() data.Dependencies {
	return data.Dependencies{
		1, 2, 1, 3, 8,
	}
}

func commonTestlibExampleInstance() *Instance {
	r := New(0, 5, new(test.DummySM))
	i := NewInstance(r, conflictNotFound+1)
	i.cmds = data.Commands{
		data.Command("world"),
	}

	i.deps = data.Dependencies{
		1, 2, 3, 4, 5,
	}

	i.seq = 42
	i.initRecoveryInfo()
	return i
}

func commonTestlibExampleNilStatusInstance() *Instance {
	return commonTestlibExampleInstance()
}
func commonTestlibExamplePreAcceptedInstance() *Instance {
	i := commonTestlibExampleInstance()
	i.status = preAccepted
	i.ballot = i.replica.makeInitialBallot()
	return i
}
func commonTestlibExampleAcceptedInstance() *Instance {
	i := commonTestlibExampleInstance()
	i.status = accepted
	i.ballot = i.replica.makeInitialBallot()
	return i
}
func commonTestlibExampleCommittedInstance() *Instance {
	i := commonTestlibExampleInstance()
	i.status = committed
	i.ballot = i.replica.makeInitialBallot()
	return i
}
func commonTestlibExamplePreParingInstance() *Instance {
	i := commonTestlibExampleInstance()
	i.status = preparing
	i.ballot = i.replica.makeInitialBallot()
	return i
}

// commonTestlibGetCopyInstance returns a copy of an instance
func commonTestlibGetCopyInstance(inst *Instance) *Instance {
	copyInstanceInfo := &InstanceInfo{
		isFastPath:     inst.info.isFastPath,
		preAcceptCount: inst.info.preAcceptCount,
		acceptCount:    inst.info.acceptCount,
	}

	var maxAcceptedBallot *data.Ballot = nil
	if inst.recoveryInfo.maxAcceptedBallot != nil {
		maxAcceptedBallot = inst.recoveryInfo.maxAcceptedBallot.GetCopy()
	}
	copyReceveryInfo := &RecoveryInfo{
		preAcceptedCount:  inst.recoveryInfo.preAcceptedCount,
		replyCount:        inst.recoveryInfo.replyCount,
		maxAcceptedBallot: maxAcceptedBallot,
		cmds:              inst.recoveryInfo.cmds.GetCopy(),
		deps:              inst.recoveryInfo.deps.GetCopy(),
		status:            inst.recoveryInfo.status,
		formerStatus:      inst.recoveryInfo.formerStatus,
	}

	return &Instance{
		cmds:         inst.cmds.GetCopy(),
		seq:          inst.seq,
		deps:         inst.deps.GetCopy(),
		status:       inst.status,
		ballot:       inst.ballot.GetCopy(),
		info:         copyInstanceInfo,
		recoveryInfo: copyReceveryInfo,
		replica:      inst.replica,
		id:           inst.id,
		executed:     inst.executed,
	}
}

func TestNewInstance(t *testing.T) {
	expectedReplicaId := uint8(0)
	expectedInstanceId := uint64(1)
	r := New(expectedReplicaId, 5, new(test.DummySM))
	i := NewInstance(r, expectedInstanceId)
	assert.Equal(t, i.replica.Id, expectedReplicaId)
	assert.Equal(t, i.id, expectedInstanceId)
	assert.Equal(t, i.deps, i.replica.makeInitialDeps())
}

// ************************
// ****** Nil Status ******
// ************************

// If a nilstatus instance receives propose, it should change its status to
// preaccepted, return (broadcastAction, pre-accept message) and setup relevant
// information.
// The instance should also be ready to receive pre-accept reply. That means the
// relevant info should be set.
func TestNilStatusProcessPropose(t *testing.T) {
	p := &data.Propose{
		Cmds: commonTestlibExampleCommands(),
	}

	instWithBallot := commonTestlibExampleNilStatusInstance()
	instWithBallot.ballot = instWithBallot.replica.makeInitialBallot()
	// test panics not freshly created nilStatus instance
	assert.Panics(t, func() { instWithBallot.nilStatusProcess(p) })

	// test panics instance's status is not nilStatus
	preAcceptedInstance := commonTestlibExamplePreAcceptedInstance()
	assert.Panics(t, func() { preAcceptedInstance.nilStatusProcess(p) })

	i := commonTestlibExampleNilStatusInstance()
	// test panics empty propose
	assert.Panics(t, func() { i.nilStatusProcess(&data.Propose{}) })

	action, m := i.nilStatusProcess(p)
	if !assert.IsType(t, &data.PreAccept{}, m) {
		t.Fatal("")
	}

	pa := m.(*data.PreAccept)
	assert.Equal(t, i.status, preAccepted)
	assert.Equal(t, action, fastQuorumAction)

	assert.True(t, assert.ObjectsAreEqual(pa, &data.PreAccept{
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Cmds:       commonTestlibExampleCommands(),
		Seq:        0,
		Deps:       i.deps,
		Ballot:     i.replica.makeInitialBallot(),
	}))

	assert.Equal(t, i.info.preAcceptCount, 0)
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

// ************************
// ****** PREACCEPTED *****
// ************************

// TestPreAcceptedProcessStatus tests
// if preAcceptedProcess panics as expected
func TestPreAcceptedProcessStatus(t *testing.T) {
	inst := commonTestlibExampleAcceptedInstance()
	ac := &data.Accept{}
	assert.Panics(t, func() { inst.preAcceptedProcess(ac) })
}

// TestPreAcceptedProcessWithRejectPreAccept asserts that
// when a preAccepted instance receives a pre-accept, it will reject the message
// if ballot > self ballot,
func TestPreAcceptedProcessWithRejectPreAccept(t *testing.T) {
	inst := commonTestlibExamplePreAcceptedInstance()

	smallerBallot := data.NewBallot(2, 2, inst.replica.Id)
	largerBallot := data.NewBallot(2, 4, inst.replica.Id)

	inst.ballot = largerBallot
	expectInst := commonTestlibGetCopyInstance(inst)

	// create and send a PreAccept with smaller ballot
	p := &data.PreAccept{
		Ballot: smallerBallot,
	}
	action, reply := inst.preAcceptedProcess(p)

	// expect:
	// - action: replyAction
	// - message: PreAcceptReply with ok == false, ballot = largerBallot
	// - instance: nothing changed
	assert.Equal(t, action, replyAction)
	assert.Equal(t, reply, &data.PreAcceptReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     largerBallot,
	})
	assert.Equal(t, inst, expectInst)
}

// TestPreAcceptedProcessWithHandlePreAccept asserts that
// when a pre-accepted instance receives a pre-accept, it will handle the message if
// the ballot of the message is larger than that of the instance.
func TestPreAcceptedProcessWithHandlePreAccept(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)
	expectInst.cmds = commonTestlibExampleCommands()

	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.GetIncNumCopy()

	expectedSeq := uint32(38)
	expectedDeps := data.Dependencies{5, 5, 5, 5, 5}

	inst.ballot = smallerBallot
	expectInst.ballot = largerBallot

	// create and send a pre-accept message to the instance
	p := &data.PreAccept{
		Cmds:   commonTestlibExampleCommands(),
		Deps:   expectedDeps,
		Seq:    38,
		Ballot: largerBallot,
	}
	action, m := inst.preAcceptedProcess(p)

	// expect:
	// - action: replyAction
	// - message: PreAcceptReply with ok == true, ballot == largerBallot
	// - instance: ballot == largeBallot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.PreAcceptReply{
		Ok:         true,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Seq:        expectedSeq,
		Deps:       expectedDeps,
		Ballot:     largerBallot,
	})
	assert.Equal(t, inst, expectInst)
}

// TestPreAcceptedProcessWithRejectAccept asserts that
// when a pre-accepted instance receives an accept, it will reject the message if
// the ballot of the message is smaller than that of the instance.
func TestPreAcceptedProcessWithRejectAccept(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.GetIncNumCopy()

	inst.ballot = largerBallot
	expectInst := commonTestlibGetCopyInstance(inst)

	// create and send an accept message to the instance
	ac := &data.Accept{
		Ballot: smallerBallot,
	}
	action, m := inst.preAcceptedProcess(ac)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == false, ballot == largerBallot
	// - instance: nothing changed
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.AcceptReply{
		Ok:         false,
		Ballot:     largerBallot,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
	})
	assert.Equal(t, inst, expectInst)
}

// TestPreAcceptedProcessWithHandleAccept asserts that
// when a pre-accepted instance receives an accept, it will handle the message if
// the ballot of the message is larger than that of the instance.
func TestPreAcceptedProcessWithHandleAccept(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.GetIncNumCopy()

	// create expected cmds, seq and deps
	expectCmds := commonTestlibExampleCommands()
	expectSeq := uint32(38)
	expectDeps := commonTestlibExampleDeps()

	inst.ballot = smallerBallot
	expectInst := commonTestlibGetCopyInstance(inst)
	expectInst.cmds = expectCmds
	expectInst.seq = expectSeq
	expectInst.deps = expectDeps
	expectInst.ballot = largerBallot
	expectInst.status = accepted

	// create and send an accept message to the instance
	ac := &data.Accept{
		Cmds:   expectCmds,
		Seq:    expectSeq,
		Deps:   expectDeps,
		Ballot: largerBallot,
	}
	action, m := inst.preAcceptedProcess(ac)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == true, ballot == largerBallot
	// - instance: cmds, seq and deps are changed as expected, ballot == largerBallot and status == accepted
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.AcceptReply{
		Ok:         true,
		Ballot:     largerBallot,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
	})
	assert.Equal(t, inst, expectInst)
}

// TestPreAcceptedProcessWithHandleCommit asserts that
// when a pre-accepted instance receives a commit, it will handle the commit message.
func TestPreAcceptedProcessWithHandleCommit(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	// create expected cmds, seq and deps
	expectCmds := commonTestlibExampleCommands()
	expectSeq := uint32(38)
	expectDeps := commonTestlibExampleDeps()

	expectInst := commonTestlibGetCopyInstance(inst)
	expectInst.cmds = expectCmds
	expectInst.seq = expectSeq
	expectInst.deps = expectDeps
	expectInst.status = committed

	// create and send a commit message to the instance
	cm := &data.Commit{
		Cmds: expectCmds,
		Seq:  expectSeq,
		Deps: expectDeps,
	}
	action, m := inst.preAcceptedProcess(cm)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: cmds, seq and deps are changed as expected, and status == committed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
	assert.Equal(t, inst, expectInst)
}

// **********************
// *****  ACCEPTED ******
// **********************

// TestAcceptedProcessWithRejectPreAccept asserts that
// when an accepted instance receives preaccept, it should reject it.
func TestAcceptedProcessWithRejectPreAccept(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)

	// send a pre-accept message to it
	pa := &data.PreAccept{}
	action, m := inst.acceptedProcess(pa)

	// expect:
	// - action: replyAction
	// - message: PreAcceptReply with ok == false, ballot == inst.ballot
	// - instance: nothing changed
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.PreAcceptReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     inst.ballot,
	})
	assert.Equal(t, inst, expectInst)
}

// TestAcceptedProcessWithRejectAccept asserts that
// when an accepted instance receives accept, it should reject the message if
// the ballot of the message is smaller than that of the instance.
func TestAcceptedProcessWithRejectAccept(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)
	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.GetIncNumCopy()

	inst.ballot = largerBallot
	// create an Accept message with small ballot, and send it to the instance
	ac := &data.Accept{
		Ballot: smallerBallot,
	}
	action, m := inst.acceptedProcess(ac)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == false, ballot == inst.ballot
	// - instance: ballot is updated to large ballot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.AcceptReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     inst.ballot,
	})
	expectInst.ballot = largerBallot
	assert.Equal(t, inst, expectInst)
}

// TestAcceptedProcessWithHandleAccept asserts that
// when an accepted instance receives accept, it should handle the message if
// the ballot of the message is larger than that of the instance.
func TestAcceptedProcessWithHandleAccept(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)

	// create small and large ballots
	smallBallot := inst.replica.makeInitialBallot()
	largeBallot := smallBallot.GetIncNumCopy()

	inst.ballot = smallBallot
	// create an Accept message with large ballot, and send it to the instance
	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()
	ac := &data.Accept{
		Cmds:   cmds,
		Seq:    38,
		Deps:   deps,
		Ballot: largeBallot,
	}
	action, m := inst.acceptedProcess(ac)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == true, ballot = inst.ballot
	// - instace:
	//     cmds = accept.cmds,
	//     seq = accept.seq,
	//     deps = accept.deps,
	//     ballot = accept.ballot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.AcceptReply{
		Ok:         true,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     inst.ballot,
	})

	expectInst.cmds = cmds
	expectInst.seq = 38
	expectInst.deps = deps
	expectInst.status = accepted
	expectInst.ballot = largeBallot

	assert.Equal(t, inst, expectInst)
}

// TestAcceptedProcessWithHandleCommit asserts that
// when an accepted instance receives commit, it should handle the message.
func TestAcceptedProcessWithHandleCommit(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)

	// create a commit message and send it to the instance
	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()
	cm := &data.Commit{
		Cmds:       cmds,
		Seq:        42,
		Deps:       deps,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
	}
	action, msg := inst.acceptedProcess(cm)

	// expect:
	// - action: noAction
	// - msg: nil
	// - instance: cmds == commit.cmds, seq == commit.seq, deps == commit.deps
	assert.Equal(t, action, noAction)
	assert.Equal(t, msg, nil)

	expectInst.cmds = cmds
	expectInst.seq = 42
	expectInst.deps = deps
	expectInst.status = committed

	assert.Equal(t, inst, expectInst)
}

// TestAcceptedProcessWithRejectPrepare asserts that
// when an accepted instance receives prepare, it should reject the message if
// the ballot of the prepare message is larger than that of the instance.
func TestAcceptedProcessWithRejectPrepare(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()

	// create small and large ballots
	smallBallot := inst.replica.makeInitialBallot()
	largeBallot := smallBallot.GetIncNumCopy()

	inst.ballot = largeBallot
	expectInst := commonTestlibGetCopyInstance(inst)

	// create a commit message and send it to the instance
	p := &data.Prepare{
		NeedCmdsInReply: true,
		Ballot:          smallBallot,
	}
	action, msg := inst.acceptedProcess(p)

	// expect:
	// - action: replyAction
	// - msg: PrepareReply with ok == false, ballot == largeBallot
	// - instance: nothing changed
	assert.Equal(t, action, replyAction)
	assert.Equal(t, msg, &data.PrepareReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     largeBallot,
	})
	assert.Equal(t, inst, expectInst)
}

// TestAcceptedProcessWithHandlePrepare asserts that
// when an accepted instance receives prepare, it should handle the message if
// the ballot of the prepare message is larger than that of the instance.
func TestAcceptedProcessWithHandlePrepare(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)
	// create small and large ballots
	smallBallot := inst.replica.makeInitialBallot()
	largeBallot := smallBallot.GetIncNumCopy()

	inst.ballot = smallBallot

	// create a commit message and send it to the instance
	p := &data.Prepare{
		NeedCmdsInReply: true,
		Ballot:          largeBallot,
	}
	action, msg := inst.acceptedProcess(p)

	// expect:
	// - action: replyAction
	// - msg: PrepareReply with ok == true, seq == inst.seq, cmds == inst.cmds,
	//        deps == inst.deps, ballot == largeballot, originalballot == smallballot
	// - instance: ballot = largeballot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, msg, &data.PrepareReply{
		Ok:             true,
		ReplicaId:      inst.replica.Id,
		InstanceId:     inst.id,
		Status:         accepted,
		Seq:            inst.seq,
		Cmds:           inst.cmds,
		Deps:           inst.deps,
		Ballot:         largeBallot,
		OriginalBallot: smallBallot,
	})

	expectInst.ballot = largeBallot
	assert.Equal(t, inst, expectInst)
}

// TestAcceptProcessWithNoActionOnAcceptReply asserts that
// when an accepted instance receives accept-reply, it should ignore the message if
// the ballot of the accept-reply message is smaller than that of the instance.
func TestAcceptProcessWithNoActionOnAcceptReply(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.GetIncNumCopy()

	inst.ballot = largerBallot
	expectInst := commonTestlibGetCopyInstance(inst)

	// create an accept-reply message and send it to the instance
	ar := &data.AcceptReply{
		Ballot: smallerBallot,
	}
	action, msg := inst.acceptedProcess(ar)

	// expect:
	// - action: noAction
	// - msg: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, msg, nil)
	assert.Equal(t, inst, expectInst)
}

// TestAcceptProcessWithHandleAcceptReply asserts that
// when an accepted instance receives accept-reply, it should handle the message if
// the ballot of the accept-reply message equals that of the instance.
func TestAcceptedProcessWithHandleAcceptReply(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	// modify info to make it ready to enter committed status
	inst.info.acceptCount = int(inst.replica.Size/2 - 1)

	expectInst := commonTestlibGetCopyInstance(inst)
	expectInst.info.acceptCount = int(inst.replica.Size / 2)
	expectInst.status = committed

	// create an accept-reply message and send it to the instance
	ar := &data.AcceptReply{
		Ok:     true,
		Ballot: inst.ballot.GetCopy(),
	}
	action, msg := inst.acceptedProcess(ar)

	// expect:
	// - action: broadcastAction
	// - msg: commit message
	// - instance: status == committed
	assert.Equal(t, action, broadcastAction)
	assert.Equal(t, msg, &data.Commit{
		Cmds:       inst.cmds,
		Seq:        inst.seq,
		Deps:       inst.deps,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
	})
	assert.Equal(t, inst, expectInst)
}

// TestAcceptedProcessWithNoActionOnPreAcceptReply asserts that
// when an accepted instance receives a pre-accept-reply, it should ignore the message.
func TestAcceptedProcessWithNoActionOnPreAcceptReply(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)

	// create an pre-accept-reply message and send it to the instance
	pr := &data.PreAcceptReply{}
	action, msg := inst.acceptedProcess(pr)

	// expect:
	// - action: noAction
	// - msg: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, msg, nil)
	assert.Equal(t, inst, expectInst)
}

// TestAcceptedProcessWithPrepareReply asserts that
// when an accepted instance receives a prepare-reply, it should panic if
// the instance is at its initial round, or ignore the message if the instance
// is not in its initial round.
func TestAcceptedProcessWithPrepareReply(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)

	// create a pre-accept-reply message and send it to the instance
	pr := &data.PrepareReply{}

	// expect:
	// - should get panic since the instance is at its initial round
	assert.Panics(t, func() { inst.acceptedProcess(pr) })
	assert.Equal(t, inst, expectInst)

	// increase instance's ballot
	inst.ballot = inst.ballot.GetIncNumCopy()
	expectInst.ballot = inst.ballot
	// create an pre-accept-reply message and send it to the instance
	pr = &data.PrepareReply{}
	action, msg := inst.acceptedProcess(pr)

	// expect:
	// - action: noAction
	// - msg: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, msg, nil)
	assert.Equal(t, inst, expectInst)
}

// TestAcceptedProcessWithPanic asserts that panic happens when
// 1, a non-accepted instance enters acceptedProcess()
// 2, an accepted instance receives messages that it should not receive.
func TestAcceptedProcessWithPanic(t *testing.T) {
	// 1,
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)

	// create an accept message and send it to the instance
	ac := &data.Accept{}
	// expect:
	// - should get panic since the instance is not at accepted status
	assert.Panics(t, func() { inst.acceptedProcess(ac) })
	assert.Equal(t, inst, expectInst)

	// 2,
	// create an accepted instance
	inst = commonTestlibExampleAcceptedInstance()
	expectInst = commonTestlibGetCopyInstance(inst)

	// create a propose message and send it to the instance
	pp := &data.Propose{}
	// expect:
	// - should get panic since it will fall through the `default' clause
	assert.Panics(t, func() { inst.acceptedProcess(pp) })
	assert.Equal(t, inst, expectInst)
}

// **********************
// ***** COMMITTED ******
// **********************

// When a committed instance receives:
// * pre-accept reply,
// it should ignore the message.
func TestCommittedProcessWithNoAction(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)
	// send a pre-accept message to it
	pa := &data.PreAcceptReply{}
	action, m := inst.committedProcess(pa)

	// expect:
	// - action: NoAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Nil(t, m)
	assert.Equal(t, inst, expectInst)
}

// If a committed instance receives accept, it will reject the message.
func TestCommittedProcessWithRejcetAccept(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)
	// send an Accept message to it
	a := &data.Accept{}
	action, m := inst.committedProcess(a)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == false, ballot == inst.ballot
	// - instance: nothing changed
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.AcceptReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     inst.ballot.GetCopy(),
	})
	assert.Equal(t, inst, expectInst)
}

// if a committed instance receives prepare with
// - larger ballot, reply ok = true with large ballot
// - smaller ballot, reply ok = true with small ballot.
func TestCommittedProcessWithHandlePrepare(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)

	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.GetIncNumCopy()

	// send a Prepare message to it
	expectedReply := &data.PrepareReply{
		Ok:         true,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Status:     committed,
		Cmds:       inst.cmds,
		Seq:        inst.seq,
		Deps:       inst.deps,
	}
	p := &data.Prepare{
		NeedCmdsInReply: true,
	}

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == true, ballot == message's ballot
	// - instance: nothing changed

	// handle larger ballot
	p.Ballot = largerBallot
	inst.ballot = smallerBallot
	expectedReply.Ballot = largerBallot
	expectedReply.OriginalBallot = inst.ballot

	action, m := inst.committedProcess(p)
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, expectedReply)
	assert.Equal(t, inst, expectInst)

	// handle smaller ballot
	p.Ballot = smallerBallot
	inst.ballot = largerBallot
	expectedReply.Ballot = smallerBallot.GetCopy()
	expectedReply.OriginalBallot = inst.ballot

	_, m = inst.committedProcess(p)
	assert.Equal(t, m, expectedReply)

	expectInst.ballot = largerBallot
	assert.Equal(t, inst, expectInst)
}

// committed instance should reject pre-accept messages.
func TestCommittedProcessWithRejectPreAccept(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)

	// send a PreAccept message to it
	p := &data.PreAccept{}
	action, m := inst.committedProcess(p)

	// expect:
	// - action: replyAction
	// - message: PreAcceptReply with ok == false
	// - instance: nothing changed
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.PreAcceptReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     inst.ballot,
	})
	assert.Equal(t, inst, expectInst)
}

func TestCommittedProccessWithPanic(t *testing.T) {
	// create a accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectInst := commonTestlibGetCopyInstance(inst)
	p := &data.Propose{}
	// expect:
	// - action: will panic if is not at committed status
	// - instance: nothing changed
	assert.Panics(t, func() { inst.committedProcess(p) })
	assert.Equal(t, inst, expectInst)

	// create a committed instance
	inst = commonTestlibExampleCommittedInstance()
	expectInst = commonTestlibGetCopyInstance(inst)

	// expect:
	// - action: will panic if receiving propose
	// - instance: nothing changed
	assert.Panics(t, func() { inst.committedProcess(p) })
	assert.Equal(t, inst, expectInst)
}

// **********************
// ***** PREPARING ******
// **********************

// **********************
// ***** REJECTIONS *****
// **********************

// TestRejections tests correctness of all rejection functions.
// These rejection functions have reply fields in common:
// {
//   ok: false
//   ballot: self ballot
//   Ids
// }
func TestRejections(t *testing.T) {
	inst := commonTestlibExampleInstance()
	expectedBallot := data.NewBallot(2, 2, inst.replica.Id)
	inst.ballot = expectedBallot.GetCopy()

	// reject with PreAcceptReply
	action, par := inst.rejectPreAccept()
	assert.Equal(t, action, replyAction)

	assert.True(t, assert.ObjectsAreEqual(par, &data.PreAcceptReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     expectedBallot,
	}))

	// reject with AcceptReply
	action, ar := inst.rejectAccept()
	assert.Equal(t, action, replyAction)

	assert.True(t, assert.ObjectsAreEqual(ar, &data.AcceptReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     expectedBallot,
	}))

	// reject with PrepareReply
	action, ppr := inst.rejectPrepare()
	assert.Equal(t, action, replyAction)
	assert.True(t, assert.ObjectsAreEqual(ppr, &data.PrepareReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     expectedBallot,
	}))
}

// ******************************
// ******* HANDLE MESSAGE *******
// ******************************

// It's testing `handleprepare` will return (replyaction, correct prepare-reply)
// If we send prepare which sets `needcmdsinreply` true, it should return cmds in reply.
func TestHandlePrepare(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()
	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.GetIncNumCopy()

	i.ballot = smallerBallot
	i.deps = data.Dependencies{3, 4, 5, 6, 7}

	// NeedCmdsInReply == false
	prepare := &data.Prepare{
		ReplicaId:       i.replica.Id,
		InstanceId:      i.id,
		Ballot:          largerBallot,
		NeedCmdsInReply: false,
	}

	action, reply := i.handlePrepare(prepare)

	assert.Equal(t, action, replyAction)
	// it should return {
	//   ok = true, correct status, deps, ballots
	// }
	assert.Equal(t, reply, &data.PrepareReply{
		Ok:             true,
		Seq:            42,
		Cmds:           nil,
		Status:         preAccepted,
		Deps:           i.deps.GetCopy(),
		Ballot:         largerBallot,
		OriginalBallot: smallerBallot,
		ReplicaId:      i.replica.Id,
		InstanceId:     i.id,
	})

	// NeedCmdsInReply == true
	prepare.NeedCmdsInReply = true
	i.cmds = commonTestlibExampleCommands()
	i.ballot = i.replica.makeInitialBallot()

	action, reply = i.handlePrepare(prepare)
	assert.Equal(t, action, replyAction)
	// test the reply
	assert.Equal(t, reply.Cmds, i.cmds)
}

// TestHandleCommit tests the functionality of handleCommit
// on success: handleCommit returns a no act and nil message,
// besides, the instances' status is set to commited.
// on failure: otherwise
func TestHandleCommit(t *testing.T) {
}

// TestCheckStatus tests the behaviour of checkStatus,
// - If instance is not at any status listed in checking function, it should panic.
// - If instance is at status listed, it should not panic.
func TestCheckStatus(t *testing.T) {
	i := &Instance{
		status: committed,
	}

	assert.Panics(t, func() { i.checkStatus(preAccepted, accepted, preparing) })
	assert.NotPanics(t, func() { i.checkStatus(committed) })
}
