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
		1, 2, 1, 1, 8,
	}
}

func commonTestlibUnionedDeps() data.Dependencies {
	return data.Dependencies{1, 2, 2, 3, 8}
}

func commonTestlibExampleInstance() *Instance {
	r := New(0, 5, new(test.DummySM))
	i := NewInstance(r, 0, conflictNotFound+1)
	return i
}

func commonTestlibExampleNilStatusInstance() *Instance {
	return commonTestlibExampleInstance()
}
func commonTestlibExamplePreAcceptedInstance() *Instance {
	i := commonTestlibExampleInstance()
	i.status = preAccepted
	i.ballot = i.replica.makeInitialBallot()
	i.cmds = data.Commands{
		data.Command("world"),
	}
	i.deps = data.Dependencies{
		0, 1, 2, 3, 4,
	}
	i.seq = 42
	return i
}
func commonTestlibExampleAcceptedInstance() *Instance {
	i := commonTestlibExampleInstance()
	i.status = accepted
	i.ballot = i.replica.makeInitialBallot()
	i.cmds = data.Commands{
		data.Command("world"),
	}
	i.deps = data.Dependencies{
		0, 1, 2, 3, 4,
	}
	i.seq = 42
	return i
}
func commonTestlibExampleCommittedInstance() *Instance {
	i := commonTestlibExampleInstance()
	i.status = committed
	i.ballot = i.replica.makeInitialBallot()
	i.cmds = data.Commands{
		data.Command("world"),
	}
	i.deps = data.Dependencies{
		0, 1, 2, 3, 4,
	}
	i.seq = 42
	return i
}
func commonTestlibExamplePreParingInstance() *Instance {
	i := commonTestlibExampleInstance()
	i.enterPreparing()
	return i
}

// commonTestlibCloneInstance returns a copy of an instance
func commonTestlibCloneInstance(inst *Instance) *Instance {
	copyInstanceInfo := &InstanceInfo{
		seqChanged:    inst.info.seqChanged,
		depsChanged:    inst.info.depsChanged,
		sameDepsAndSeq:  inst.info.sameDepsAndSeq,
		preAcceptCount: inst.info.preAcceptCount,
		acceptCount:    inst.info.acceptCount,
	}

	ir := inst.recoveryInfo

	copyReceveryInfo := NewRecoveryInfo()
	if inst.status == preparing {
		copyReceveryInfo = &RecoveryInfo{
			identicalCount: ir.identicalCount,
			replyCount:     ir.replyCount,
			ballot:         ir.ballot.Clone(),
			cmds:           ir.cmds.Clone(),
			deps:           ir.deps.Clone(),
			status:         ir.status,
			formerStatus:   ir.formerStatus,
			formerBallot:   ir.formerBallot,
		}
	}

	return &Instance{
		cmds:         inst.cmds.Clone(),
		seq:          inst.seq,
		deps:         inst.deps.Clone(),
		status:       inst.status,
		ballot:       inst.ballot.Clone(),
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
	i := NewInstance(r, expectedReplicaId, expectedInstanceId)
	assert.Equal(t, i.replica.Id, expectedReplicaId)
	assert.Equal(t, i.rowId, expectedReplicaId)
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

	assert.Equal(t, pa, &data.PreAccept{
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Cmds:       commonTestlibExampleCommands(),
		Seq:        0,
		Deps:       i.deps,
		Ballot:     i.replica.makeInitialBallot(),
	})

	assert.Equal(t, i.info.preAcceptCount, 0)
	assert.True(t, i.info.sameDepsAndSeq)
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

// TestPreAcceptedProcessWithRejectPreAccept asserts that
// On receiving smaller ballot pre-accept, preAccepted instance will reject it.
func TestPreAcceptedProcessWithRejectPreAccept(t *testing.T) {
	inst := commonTestlibExamplePreAcceptedInstance()

	smallerBallot := data.NewBallot(2, 2, inst.replica.Id)
	largerBallot := data.NewBallot(2, 4, inst.replica.Id)

	inst.ballot = largerBallot
	expectedInst := commonTestlibCloneInstance(inst)

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
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithHandlePreAccept asserts that
// On receiving larger ballot pre-accept, preaccepted instance will handle and
// reply with the correct seq, deps
func TestPreAcceptedProcessWithHandlePreAccept(t *testing.T) {
	inst := commonTestlibExamplePreAcceptedInstance()

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	expectedSeq := inst.seq + 1
	expectedDeps := data.Dependencies{5, 0, 0, 0, 0}
	expectedCmds := commonTestlibExampleCommands()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.cmds = expectedCmds
	expectedInst.seq = expectedSeq
	expectedInst.deps = expectedDeps
	expectedInst.ballot = largerBallot

	// This is the pre-accept with larger ballot than instance.
	// instance should handle it.
	p := &data.PreAccept{
		Cmds:   expectedCmds,
		Deps:   expectedDeps,
		Seq:    expectedSeq,
		Ballot: largerBallot,
	}
	action, reply := inst.preAcceptedProcess(p)

	// expect:
	// - action: replyAction
	// - message: PreAcceptReply with ok == true, ballot == largerBallot
	// - instance: ballot == largeBallot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, reply, &data.PreAcceptReply{
		Ok:         true,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Seq:        expectedSeq,
		Deps:       expectedDeps,
		Ballot:     largerBallot,
	})
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithRejectAccept asserts that
// On receiving smaller ballot accept, preAccepted instance will reject it.
func TestPreAcceptedProcessWithRejectAccept(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = largerBallot
	expectedInst := commonTestlibCloneInstance(inst)

	// create and send an accept message to the instance
	ac := &data.Accept{
		Ballot: smallerBallot,
	}
	action, reply := inst.preAcceptedProcess(ac)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == false, ballot == largerBallot
	// - instance: nothing changed
	assert.Equal(t, action, replyAction)
	assert.Equal(t, reply, &data.AcceptReply{
		Ok:         false,
		Ballot:     largerBallot,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
	})
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithHandleAccept asserts that
// On receiving equal or larger ballot accept, preAccepted instance will handle it:
// - correct accept rely message,
// - instance status is changed to accepted
func TestPreAcceptedProcessWithHandleAccept(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = smallerBallot

	// create expected cmds, seq and deps
	expectedCmds := commonTestlibExampleCommands()
	expectedSeq := uint32(38)
	expectedDeps := commonTestlibExampleDeps()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.cmds = expectedCmds
	expectedInst.seq = expectedSeq
	expectedInst.deps = expectedDeps
	expectedInst.status = accepted

	// create and send an accept message to the instance
	ac := &data.Accept{
		Cmds:   expectedCmds,
		Seq:    expectedSeq,
		Deps:   expectedDeps,
		Ballot: smallerBallot,
	}
	action, reply := inst.preAcceptedProcess(ac)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == true, ballot == largerBallot
	// - instance: cmds, seq, deps, ballot are changed, and status == accepted
	assert.Equal(t, action, replyAction)
	assert.Equal(t, reply, &data.AcceptReply{
		Ok:         true,
		Ballot:     smallerBallot,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
	})
	expectedInst.ballot = smallerBallot
	assert.Equal(t, inst, expectedInst)

	// test larger ballot accept
	inst.status = preAccepted
	ac.Ballot = largerBallot
	_, reply = inst.preAcceptedProcess(ac)
	assert.True(t, reply.(*data.AcceptReply).Ok)
	assert.Equal(t, reply.(*data.AcceptReply).Ballot, largerBallot)
	expectedInst.ballot = largerBallot
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithHandleCommit asserts that
// On receiving commit, preAccepted instance will handle it.
// 1. noaction and 2. status changed to committed
func TestPreAcceptedProcessWithHandleCommit(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	// create expected cmds, seq and deps
	expectedCmds := commonTestlibExampleCommands()
	expectedSeq := uint32(38)
	expectedDeps := commonTestlibExampleDeps()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.cmds = expectedCmds
	expectedInst.seq = expectedSeq
	expectedInst.deps = expectedDeps
	expectedInst.status = committed

	// create and send a commit message to the instance
	cm := &data.Commit{
		Cmds: expectedCmds,
		Seq:  expectedSeq,
		Deps: expectedDeps,
	}
	action, m := inst.preAcceptedProcess(cm)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: cmds, seq and deps are changed, and status == committed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithRejectPrepare asserts that
// On receiving smaller ballot prepare, preaccepted instance will reject it.
func TestPreAcceptedProcessWithRejectPrepare(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = largerBallot
	originalInst := commonTestlibCloneInstance(inst)

	// create and send a prepare message to the instance
	pr := &data.Prepare{
		Ballot: smallerBallot,
	}
	action, m := inst.preAcceptedProcess(pr)

	// expect:
	// - action: replyAction
	// - message: prepareReply with ok == false, ballot == largerBallot
	// - instance: nothing changed
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.PrepareReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     largerBallot,
	})
	assert.Equal(t, inst, originalInst)
}

// TestPreAcceptedProcessWithHandlePrepare asserts that
// on receiving larger ballot prepare, preaccepted instance will handle it.
func TestPreAcceptedProcessWithHandlePrepare(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = smallerBallot

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.ballot = largerBallot

	// create and send a prepare message to the instance
	pr := &data.Prepare{
		Ballot: largerBallot,
	}
	action, m := inst.preAcceptedProcess(pr)

	// expect:
	// - action: replyAction
	// - message: prepareReply with
	//            ok == true,
	//            ballot == largerBallot,
	//            status == preAccepted
	//            cmds, seq, deps == inst.cmds, inst.seq, inst.deps
	//            original ballot = smallerBallot
	// - instance: inst.ballot = largerBallot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.PrepareReply{
		Ok:             true,
		IsFromLeader:   true,
		ReplicaId:      inst.rowId,
		InstanceId:     inst.id,
		Status:         preAccepted,
		Seq:            inst.seq,
		Cmds:           inst.cmds,
		Deps:           inst.deps,
		Ballot:         largerBallot,
		OriginalBallot: smallerBallot,
	})
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithIgorePreAcceptReply asserts that
// on receiving smaller ballot preaccept reply, preaccepted will ignore it.
func TestPreAcceptedProcessWithIgorePreAcceptReply(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = largerBallot
	originalInst := commonTestlibCloneInstance(inst)

	// create and send a prepare message to the instance
	pr := &data.PreAcceptReply{
		Ballot: smallerBallot,
	}
	action, m := inst.preAcceptedProcess(pr)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
	assert.Equal(t, inst, originalInst)
}

// TestPreAcceptedProcessWithHandlePreAcceptReply asserts that
// on receiving corresponding preacept reply, and it replies with different deps,
// and it reaches majority votes limit, preaccepted instance should handle it
// - enter accept phase, broadcast accepts.
func TestPreAcceptedProcessWithHandlePreAcceptReply(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	expectedSeq := uint32(inst.seq + 1)
	expectedDeps := commonTestlibUnionedDeps()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.status = accepted
	expectedInst.seq = expectedSeq
	expectedInst.deps = expectedDeps

	inst.info.preAcceptCount = inst.replica.quorum() - 1

	// create and send a prepare message to the instance
	pr := &data.PreAcceptReply{
		Ballot: inst.ballot,
		Deps:   commonTestlibExampleDeps(),
		Seq:    expectedSeq,
	}
	action, m := inst.preAcceptedProcess(pr)

	// expect:
	// - action: broadcastAction
	// - message: accept with correct cmds, seq, deps
	// - instance: status == accepted
	assert.Equal(t, action, broadcastAction)
	assert.Equal(t, m, &data.Accept{
		Cmds:       inst.cmds,
		Seq:        expectedSeq,
		Deps:       expectedDeps,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     inst.ballot,
	})
	assert.Equal(t, inst, expectedInst)
}

// **************
// The following two tests focus on receiving preaccept-ok/-reply to test different
// cases and fast/slow path of epaxos
// **************

func TestPreAcceptedFastPath(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()
	// shold be initial round
	assert.True(t, i.ballot.IsInitialBallot())

	p := &data.PreAcceptOk{InstanceId: i.id}

	i.preAcceptedProcess(p)
}

func TestPreAcceptedSlowPath(t *testing.T) {
}

// TestPreAcceptedProcessWithHandlePreAcceptOk asserts that
// when a pre-accepted instance receives a pre-accept-ok message, it
// will handle the message if the instance is at its initial round.
// Otherwise it should panic.
func TestPreAcceptedProcessWithHandlePreAcceptOk(t *testing.T) {
	// create a pre-accepted instance
	i := commonTestlibExamplePreAcceptedInstance()

	expectedInst := commonTestlibCloneInstance(i)
	expectedInst.status = committed
	expectedInst.info.preAcceptCount = i.replica.fastQuorum()

	i.info.preAcceptCount = i.replica.fastQuorum() - 1

	// create and send a prepare message to the instance
	pr := &data.PreAcceptOk{}
	action, m := i.preAcceptedProcess(pr)

	// expect:
	// - action: broadcastAction
	// - message: accept with correct cmds, seq, deps
	// - instance: status == accepted
	assert.Equal(t, action, broadcastAction)
	assert.Equal(t, m, &data.Commit{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       i.cmds,
		Seq:        i.seq,
		Deps:       i.deps,
	})

	assert.Equal(t, i, expectedInst)
}

// TestPreAcceptedProcessWithPrepareReply asserts that
// on receiving prepare-reply message, preaccepted instance will:
// 1, panic if the instance is at its initial round
// 2, ignore the message if the instance is not at its initial round
func TestPreAcceptedProcessWithPrepareReply(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	// create small and large ballots
	initialBallot := inst.replica.makeInitialBallot()
	largerBallot := initialBallot.IncNumClone()

	inst.ballot = initialBallot

	// 1,
	// create a prepare-reply and send it to the intance
	pr := &data.PrepareReply{}
	// expect: panic on receiving the prepare-reply message
	assert.Panics(t, func() { inst.preAcceptedProcess(pr) })

	// 2,
	// update instance's ballot
	inst.ballot = largerBallot
	expectedInst := commonTestlibCloneInstance(inst)

	// create a prepare-reply and send it to the intance
	pr = &data.PrepareReply{}
	action, msg := inst.preAcceptedProcess(pr)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, msg, nil)
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithPanic asserts that
// the preAcceptedProcess func will panic if
// 1, the instance is not at preAccepted status
// 2, it receive unexpected messages such as accept-reply (because one
// instance cannot revert from accepted to preAccepted status) or propose
func TestPreAcceptedProcessWithPanic(t *testing.T) {
	// 1, should panic if the instance is not at preAccepted status
	inst := commonTestlibExampleAcceptedInstance()
	cm := &data.Commit{}
	assert.Panics(t, func() { inst.preAcceptedProcess(cm) })

	// 2, should panic if the instance receives accept-reply or propose messages
	inst = commonTestlibExamplePreAcceptedInstance()
	ar := &data.AcceptReply{}
	pp := &data.Propose{}

	assert.Panics(t, func() { inst.preAcceptedProcess(ar) })
	assert.Panics(t, func() { inst.preAcceptedProcess(pp) })
}

// **********************
// *****  ACCEPTED ******
// **********************

// TestAcceptedProcessWithRejectPreAccept asserts that
// when an accepted instance receives preaccept, it should reject it.
func TestAcceptedProcessWithRejectPreAccept(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectedInst := commonTestlibCloneInstance(inst)

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
	assert.Equal(t, inst, expectedInst)
}

// TestAcceptedProcessWithRejectAccept asserts that
// on receiving smaller ballot accept, accepted instance will reject it.
func TestAcceptedProcessWithRejectAccept(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectedInst := commonTestlibCloneInstance(inst)

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

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
	expectedInst.ballot = largerBallot
	assert.Equal(t, inst, expectedInst)
}

// TestAcceptedProcessWithHandleAccept asserts that
// on receiving larger ballot accept, accepted instance will handle it.
func TestAcceptedProcessWithHandleAccept(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()

	// create small and large ballots
	smallBallot := inst.replica.makeInitialBallot()
	largeBallot := smallBallot.IncNumClone()

	inst.ballot = smallBallot

	// create an Accept message with large ballot, and send it to the instance
	seq := inst.seq + 1
	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.cmds = cmds
	expectedInst.seq = seq
	expectedInst.deps = deps
	expectedInst.status = accepted
	expectedInst.ballot = largeBallot

	ac := &data.Accept{
		Cmds:   cmds,
		Seq:    seq,
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

	assert.Equal(t, inst, expectedInst)
}

// TestAcceptedProcessWithHandleCommit asserts that
// when an accepted instance receives commit, it should handle the message.
func TestAcceptedProcessWithHandleCommit(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()

	seq := uint32(inst.seq)
	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.cmds = cmds
	expectedInst.seq = seq
	expectedInst.deps = deps
	expectedInst.status = committed

	// create a commit message and send it to the instance
	cm := &data.Commit{
		Cmds:       cmds,
		Seq:        seq,
		Deps:       deps,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
	}
	action, m := inst.acceptedProcess(cm)

	// expect:
	// - action: noAction
	// - msg: nil
	// - instance: cmds == commit.cmds, seq == commit.seq, deps == commit.deps
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
	assert.Equal(t, inst, expectedInst)
}

// TestAcceptedProcessWithRejectPrepare asserts that
// when an accepted instance receives prepare, it should reject the message if
// the ballot of the prepare message is larger than that of the instance.
func TestAcceptedProcessWithRejectPrepare(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()

	// create small and large ballots
	smallBallot := inst.replica.makeInitialBallot()
	largeBallot := smallBallot.IncNumClone()

	inst.ballot = largeBallot
	originalInst := commonTestlibCloneInstance(inst)

	// create a commit message and send it to the instance
	p := &data.Prepare{
		Ballot: smallBallot,
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
	assert.Equal(t, inst, originalInst)
}

// TestAcceptedProcessWithHandlePrepare asserts that
// on receiving larger ballot prepare, accepted instance should:
// - handle it
// - instance unchanged
func TestAcceptedProcessWithHandlePrepare(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectedInst := commonTestlibCloneInstance(inst)
	// create small and large ballots
	smallBallot := inst.replica.makeInitialBallot()
	largeBallot := smallBallot.IncNumClone()

	inst.ballot = smallBallot

	// create a commit message and send it to the instance
	p := &data.Prepare{
		Ballot: largeBallot,
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
		IsFromLeader:   true,
		ReplicaId:      inst.rowId,
		InstanceId:     inst.id,
		Status:         accepted,
		Seq:            inst.seq,
		Cmds:           inst.cmds,
		Deps:           inst.deps,
		Ballot:         largeBallot,
		OriginalBallot: smallBallot,
	})

	expectedInst.ballot = largeBallot
	assert.Equal(t, inst, expectedInst)
}

// TestAcceptedProcessWithNoActionOnAcceptReply asserts that
// when an accepted instance receives accept-reply, it should ignore the message if
// the ballot of the accept-reply message is smaller than that of the instance.
func TestAcceptedProcessWithNoActionOnAcceptReply(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = largerBallot
	originalInst := commonTestlibCloneInstance(inst)

	// create an accept-reply message and send it to the instance
	ar := &data.AcceptReply{
		Ballot: smallerBallot,
	}
	action, m := inst.acceptedProcess(ar)

	// expect:
	// - action: noAction
	// - msg: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
	assert.Equal(t, inst, originalInst)
}

// TestAcceptProcessWithHandleAcceptReply asserts that
// when an accepted instance receives accept-reply, it should handle the message if
// the ballot of the accept-reply message equals that of the instance.
func TestAcceptedProcessWithHandleAcceptReply(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	// modify info to make it ready to enter committed status
	inst.info.acceptCount = inst.replica.quorum() - 1

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.info.acceptCount = inst.replica.quorum()
	expectedInst.status = committed

	// create an accept-reply message and send it to the instance
	ar := &data.AcceptReply{
		Ok:     true,
		Ballot: inst.ballot.Clone(),
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
	assert.Equal(t, inst, expectedInst)
}

// TestAcceptedProcessWithNoActionOnPreAcceptReply asserts that
// when an accepted instance receives a pre-accept-reply, it should ignore the message.
func TestAcceptedProcessWithNoActionOnPreAcceptReply(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectedInst := commonTestlibCloneInstance(inst)

	// create an pre-accept-reply message and send it to the instance
	pr := &data.PreAcceptReply{}
	action, msg := inst.acceptedProcess(pr)

	// expect:
	// - action: noAction
	// - msg: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, msg, nil)
	assert.Equal(t, inst, expectedInst)
}

// TestAcceptedProcessWithPrepareReply asserts that
// when an accepted instance receives a prepare-reply, it should panic if
// the instance is at its initial round, or ignore the message if the instance
// is not in its initial round.
func TestAcceptedProcessWithPrepareReply(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectedInst := commonTestlibCloneInstance(inst)

	// create a pre-accept-reply message and send it to the instance
	pr := &data.PrepareReply{}

	// expect:
	// - should get panic since the instance is at its initial round
	assert.Panics(t, func() { inst.acceptedProcess(pr) })
	assert.Equal(t, inst, expectedInst)

	// increase instance's ballot
	inst.ballot = inst.ballot.IncNumClone()
	expectedInst.ballot = inst.ballot
	// create an pre-accept-reply message and send it to the instance
	pr = &data.PrepareReply{}
	action, msg := inst.acceptedProcess(pr)

	// expect:
	// - action: noAction
	// - msg: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, msg, nil)
	assert.Equal(t, inst, expectedInst)
}

// TestAcceptedProcessWithPanic asserts that panic happens when
// 1, a non-accepted instance enters acceptedProcess()
// 2, an accepted instance receives messages that it should not receive.
func TestAcceptedProcessWithPanic(t *testing.T) {
	// 1,
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()
	expectedInst := commonTestlibCloneInstance(inst)

	// create an accept message and send it to the instance
	ac := &data.Accept{}
	// expect:
	// - should get panic since the instance is not at accepted status
	assert.Panics(t, func() { inst.acceptedProcess(ac) })
	assert.Equal(t, inst, expectedInst)

	// 2,
	// create an accepted instance
	inst = commonTestlibExampleAcceptedInstance()
	expectedInst = commonTestlibCloneInstance(inst)

	// create a propose message and send it to the instance
	pp := &data.Propose{}
	// expect:
	// - should get panic since it will fall through the `default' clause
	assert.Panics(t, func() { inst.acceptedProcess(pp) })
	assert.Equal(t, inst, expectedInst)
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
	expectedInst := commonTestlibCloneInstance(inst)
	// send a pre-accept message to it
	pa := &data.PreAcceptReply{}
	action, m := inst.committedProcess(pa)

	// expect:
	// - action: NoAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Nil(t, m)
	assert.Equal(t, inst, expectedInst)
}

// If a committed instance receives accept, it will reject the message.
func TestCommittedProcessWithRejcetAccept(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	expectedInst := commonTestlibCloneInstance(inst)
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
		Ballot:     inst.ballot.Clone(),
	})
	assert.Equal(t, inst, expectedInst)
}

// if a committed instance receives prepare with
// - larger ballot, reply ok = true with large ballot
// - smaller ballot, reply ok = true with small ballot.
func TestCommittedProcessWithHandlePrepare(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	expectedInst := commonTestlibCloneInstance(inst)

	// create small and large ballots
	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	// send a Prepare message to it
	expectedReply := &data.PrepareReply{
		Ok:           true,
		ReplicaId:    inst.rowId,
		IsFromLeader: true,
		InstanceId:   inst.id,
		Status:       committed,
		Cmds:         inst.cmds,
		Seq:          inst.seq,
		Deps:         inst.deps,
	}
	p := &data.Prepare{}

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
	assert.Equal(t, inst, expectedInst)

	// handle smaller ballot
	p.Ballot = smallerBallot
	inst.ballot = largerBallot
	expectedReply.Ballot = smallerBallot.Clone()
	expectedReply.OriginalBallot = inst.ballot

	_, m = inst.committedProcess(p)
	assert.Equal(t, m, expectedReply)

	expectedInst.ballot = largerBallot
	assert.Equal(t, inst, expectedInst)
}

// committed instance should reject pre-accept messages.
func TestCommittedProcessWithRejectPreAccept(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	expectedInst := commonTestlibCloneInstance(inst)

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
	assert.Equal(t, inst, expectedInst)
}

func TestCommittedProccessWithPanic(t *testing.T) {
	// create a accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectedInst := commonTestlibCloneInstance(inst)
	p := &data.Propose{}
	// expect:
	// - action: will panic if is not at committed status
	// - instance: nothing changed
	assert.Panics(t, func() { inst.committedProcess(p) })
	assert.Equal(t, inst, expectedInst)

	// create a committed instance
	inst = commonTestlibExampleCommittedInstance()
	expectedInst = commonTestlibCloneInstance(inst)

	// expect:
	// - action: will panic if receiving propose
	// - instance: nothing changed
	assert.Panics(t, func() { inst.committedProcess(p) })
	assert.Equal(t, inst, expectedInst)
}

// **********************
// ***** PREPARING ******
// **********************

// If a preparing instance as nilstatus handles
// - committed reply, it should set its recovery info according to the reply
// - accepted reply, it should set its recovery info according to the reply
// - pre-accepted reply, it should set its recovery info according to the reply
// - nilstatus reply, ignore
func TestNilStatusPreparingHandlePrepareReply(t *testing.T) {
	// committed reply
	i := commonTestlibExamplePreParingInstance()
	ir := i.recoveryInfo

	originalBallot := i.replica.makeInitialBallot()
	messageBallot := i.ballot.Clone()

	p := &data.PrepareReply{
		Ok:             true,
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Cmds:           commonTestlibExampleCommands(),
		Seq:            i.seq + 1,
		Deps:           commonTestlibExampleDeps(),
		Ballot:         messageBallot,
		OriginalBallot: originalBallot,
		IsFromLeader:   false,
	}
	p.Status = committed

	assert.Equal(t, ir.status, nilStatus)
	assert.NotEqual(t, ir.cmds, p.Cmds)

	i.handlePrepareReply(p)

	assert.Equal(t, ir.status, committed)
	assert.Equal(t, ir.cmds, p.Cmds)

	// accepted reply
	i = commonTestlibExamplePreParingInstance()
	ir = i.recoveryInfo
	p.Status = accepted

	i.handlePrepareReply(p)
	assert.Equal(t, ir.status, accepted)

	// pre-accepted reply
	i = commonTestlibExamplePreParingInstance()
	ir = i.recoveryInfo
	p.Status = preAccepted

	i.handlePrepareReply(p)
	assert.Equal(t, ir.status, preAccepted)

	// nilstatus reply
	i = commonTestlibExamplePreParingInstance()
	ir = i.recoveryInfo
	p.Status = nilStatus
	i.handlePrepareReply(p)
	assert.Equal(t, ir.status, nilStatus)
	assert.NotEqual(t, ir.cmds, p.Cmds)
}

// If a preparing instance as preaccepted handles prepare reply of
// - committed, it should set its recovery info according to the reply
// - accepted, it should set its recovery info according to the reply
// - nilstatus, ignore
// - pre-accepted, where original ballot compared to recovery ballot is
// - - larger, set accordingly
// - - smaller, ignore
// - - same, but
// - - - not initial or initial but from leader, ignore
// Finally, send N/2 identical initial pre-accepted back, it should
// broadcast accepts.
func TestPreAcceptedPreparingHandlePrepareReply(t *testing.T) {
	// committed
	i := commonTestlibExamplePreAcceptedInstance()
	i.enterPreparing()
	ir := i.recoveryInfo

	assert.Equal(t, ir.ballot.Number(), uint64(0))

	originalBallot := ir.ballot
	messageBallot := i.ballot.Clone()

	p := &data.PrepareReply{
		Ok:             true,
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Cmds:           commonTestlibExampleCommands(),
		Seq:            i.seq,
		Deps:           commonTestlibExampleDeps(),
		Ballot:         messageBallot,
		OriginalBallot: originalBallot,
		IsFromLeader:   false,
	}
	p.Status = committed

	i.handlePrepareReply(p)
	assert.Equal(t, ir.status, committed)
	assert.Equal(t, ir.cmds, p.Cmds)

	// accepted
	i = commonTestlibExamplePreAcceptedInstance()
	i.enterPreparing()
	ir = i.recoveryInfo
	p.Status = accepted

	i.handlePrepareReply(p)
	assert.Equal(t, ir.status, accepted)

	// nilstatus
	i = commonTestlibExamplePreAcceptedInstance()
	i.enterPreparing()
	ir = i.recoveryInfo
	p.Status = nilStatus

	i.handlePrepareReply(p)
	assert.Equal(t, ir.status, preAccepted)
	assert.NotEqual(t, ir.cmds, p.Cmds)

	// preaccepted
	i = commonTestlibExamplePreAcceptedInstance()
	i.enterPreparing()
	ir = i.recoveryInfo
	p.Status = preAccepted

	// larger original ballot
	p.OriginalBallot = originalBallot.IncNumClone()
	i.handlePrepareReply(p)
	assert.Equal(t, ir.cmds, p.Cmds)

	// smaller original ballot
	i = commonTestlibExamplePreAcceptedInstance()
	i.enterPreparing()
	ir = i.recoveryInfo
	ir.ballot = originalBallot.IncNumClone()
	i.handlePrepareReply(p)
	assert.NotEqual(t, ir.cmds, p.Cmds)

	// same original ballot but different deps or not initial or from leader
	// It should not change its identicalcount

	i = commonTestlibExamplePreAcceptedInstance()
	i.enterPreparing()
	ir = i.recoveryInfo
	p.Cmds, p.OriginalBallot = ir.cmds, ir.ballot
	assert.NotEqual(t, ir.deps, p.Deps) // different deps
	i.handlePrepareReply(p)
	assert.Equal(t, ir.identicalCount, 0)

	i = commonTestlibExamplePreAcceptedInstance()
	i.enterPreparing()
	ir = i.recoveryInfo
	ir.ballot.IncNumber()
	p.Cmds, p.Deps = ir.cmds, ir.deps
	p.OriginalBallot = ir.ballot.IncNumClone() // non initial
	i.handlePrepareReply(p)
	assert.Equal(t, ir.identicalCount, 0)

	i = commonTestlibExamplePreAcceptedInstance()
	i.enterPreparing()
	ir = i.recoveryInfo
	p.Cmds, p.Deps, p.OriginalBallot = ir.cmds, ir.deps, ir.ballot
	p.IsFromLeader = true // from leader
	i.handlePrepareReply(p)
	assert.Equal(t, ir.identicalCount, 0)

	// receiving N/2 identical initial, broadcast accepts
	i = commonTestlibExamplePreAcceptedInstance()
	i.enterPreparing()
	ir = i.recoveryInfo
	p.Cmds, p.Deps, p.OriginalBallot = ir.cmds, ir.deps, ir.ballot
	p.IsFromLeader = false

	for count := 0; count < i.replica.quorum(); count++ {
		action, msg := i.handlePrepareReply(p)
		if count != i.replica.quorum()-1 {
			assert.Equal(t, action, noAction)
			assert.Equal(t, ir.identicalCount, count+1)
		} else {
			assert.Equal(t, action, broadcastAction)
			ac := msg.(*data.Accept)
			assert.Equal(t, i.status, accepted)
			assert.Equal(t, ac.Cmds, p.Cmds)
		}
	}
}

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
	inst.ballot = expectedBallot.Clone()

	// reject with PreAcceptReply
	action, par := inst.rejectPreAccept()
	assert.Equal(t, action, replyAction)

	assert.Equal(t, par, &data.PreAcceptReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     expectedBallot,
	})

	// reject with AcceptReply
	action, ar := inst.rejectAccept()
	assert.Equal(t, action, replyAction)

	assert.Equal(t, ar, &data.AcceptReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     expectedBallot,
	})

	// reject with PrepareReply
	action, ppr := inst.rejectPrepare()
	assert.Equal(t, action, replyAction)
	assert.Equal(t, ppr, &data.PrepareReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     expectedBallot,
	})
}

// ******************************
// ******* HANDLE MESSAGE *******
// ******************************

// It's testing `handleprepare` will return (replyaction, correct prepare-reply)
func TestHandlePrepare(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()
	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	i.ballot = smallerBallot
	i.deps = data.Dependencies{3, 4, 5, 6, 7}

	prepare := &data.Prepare{
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Ballot:     largerBallot,
	}

	action, reply := i.handlePrepare(prepare)

	assert.Equal(t, action, replyAction)
	// it should return {
	//   ok = true, correct status, deps, ballots
	// }
	assert.Equal(t, reply, &data.PrepareReply{
		Ok:             true,
		Seq:            42,
		Cmds:           i.cmds,
		Status:         preAccepted,
		Deps:           i.deps,
		Ballot:         largerBallot,
		OriginalBallot: smallerBallot,
		ReplicaId:      i.rowId,
		IsFromLeader:   true,
		InstanceId:     i.id,
	})

	i.cmds = commonTestlibExampleCommands()
	i.ballot = i.replica.makeInitialBallot()

	action, reply = i.handlePrepare(prepare)
	assert.Equal(t, action, replyAction)
	// test the reply
	assert.Equal(t, reply.Cmds, i.cmds)
}

// If nilstatus, preacepted, accepted, preparing instances handles commit,
// they should be changed to committed and accept things from commit message.
func TestHandleCommit(t *testing.T) {
	i := commonTestlibExampleAcceptedInstance()

	cm := &data.Commit{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       commonTestlibExampleCommands(),
		Seq:        i.seq + 1,
		Deps:       commonTestlibExampleDeps(),
	}
	action, msg := i.handleCommit(cm)

	assert.Equal(t, i.status, committed)
	assert.Equal(t, i.cmds, cm.Cmds)
	assert.Equal(t, i.seq, cm.Seq)
	assert.Equal(t, i.deps, cm.Deps)
	assert.Equal(t, action, noAction)
	assert.Equal(t, msg, nil)

	i = commonTestlibExamplePreAcceptedInstance()
	i.handleCommit(cm)
	assert.Equal(t, i.status, committed)

	i = commonTestlibExamplePreParingInstance()
	i.handleCommit(cm)
	assert.Equal(t, i.status, committed)

	i = commonTestlibExampleNilStatusInstance()
	i.handleCommit(cm)
	assert.Equal(t, i.status, committed)
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
