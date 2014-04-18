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
	deps := commonTestlibExampleDeps()
	deps.Union(data.Dependencies{
		0, 1, 2, 3, 4,
	})
	return deps
}

func commonTestlibExampleReplica() *Replica {
	param := &Param{
		ReplicaId:    0,
		Size:         5,
		StateMachine: new(test.DummySM),
	}
	r, _ := New(param)
	return r
}

func commonTestlibExampleInstance() *Instance {
	param := &Param{
		ReplicaId:    0,
		Size:         5,
		StateMachine: new(test.DummySM),
	}
	r, _ := New(param)
	i := NewInstance(r, r.Id+1, conflictNotFound+1) // make rowId different with i.replica.Id
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
	return i
}

func commonTestlibExamplePreparingInstance() *Instance {
	i := commonTestlibExampleInstance()
	i.enterPreparing()
	return i
}

// commonTestlibCloneInstance returns a copy of an instance
func commonTestlibCloneInstance(inst *Instance) *Instance {
	copyInstanceInfo := &InstanceInfo{
		samePreAcceptReplies: inst.info.samePreAcceptReplies,
		preAcceptOkCount:     inst.info.preAcceptOkCount,
		preAcceptReplyCount:  inst.info.preAcceptReplyCount,
		acceptReplyCount:     inst.info.acceptReplyCount,
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
		deps:         inst.deps.Clone(),
		status:       inst.status,
		ballot:       inst.ballot.Clone(),
		info:         copyInstanceInfo,
		recoveryInfo: copyReceveryInfo,
		replica:      inst.replica,
		rowId:        inst.rowId,
		id:           inst.id,
		executed:     inst.executed,
		lastTouched:  inst.lastTouched,
	}
}

func TestNewInstance(t *testing.T) {
	expectedReplicaId := uint8(0)
	expectedInstanceId := uint64(1)
	param := &Param{
		ReplicaId:    expectedReplicaId,
		Size:         5,
		StateMachine: new(test.DummySM),
	}
	r, _ := New(param)
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
func TestNilStatusProcessWithHandlePropose(t *testing.T) {
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
	i.rowId = i.replica.Id // to avoid panic

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
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       commonTestlibExampleCommands(),
		Deps:       i.deps,
		Ballot:     i.replica.makeInitialBallot(),
	})

	assert.Equal(t, i.info.preAcceptReplyCount, 0)
	assert.True(t, i.info.samePreAcceptReplies)
}

// This function asserts that one instance will ignore a pre-accept
// message if the ballot of the message is smaller.
func TestNilStatusProcessWithIgnorePreAccept(t *testing.T) {
	inst := commonTestlibExampleNilStatusInstance()

	smallerBallot := data.NewBallot(2, 2, inst.replica.Id)
	largerBallot := data.NewBallot(2, 4, inst.replica.Id)

	inst.ballot = largerBallot

	p := &data.PreAccept{
		Ballot: smallerBallot,
	}

	action, m := inst.nilStatusProcess(p)

	// expect:
	// - action: noAction
	// - message: nil
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
}

// This function asserts that one instance will handle a pre-accept
// message if the ballot of the message is larger.
func TestNilStatusProcessWithHandlePreAccept(t *testing.T) {
	inst := commonTestlibExampleNilStatusInstance()

	smallerBallot := data.NewBallot(2, 2, inst.replica.Id)
	largerBallot := data.NewBallot(2, 4, inst.replica.Id)

	expectedDeps := data.Dependencies{5, 0, 0, 0, 0}
	expectedCmds := commonTestlibExampleCommands()

	inst.ballot = smallerBallot

	p := &data.PreAccept{
		Cmds:   expectedCmds,
		Deps:   expectedDeps,
		Ballot: largerBallot,
	}

	action, m := inst.nilStatusProcess(p)

	// expect:
	// - action: replyAction
	// - message: preAcceptReply, Ballot = largerBallot
	//   deps == expect.deps
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.PreAcceptReply{
		ReplicaId:  inst.rowId,
		InstanceId: inst.id,
		Ballot:     largerBallot,
		Deps:       expectedDeps,
	})
}

// This function asserts that one instance will ignore an accept
// message if the ballot of the message is smaller.
func TestNilStatusProcessWithIgnoreAccept(t *testing.T) {
	inst := commonTestlibExampleNilStatusInstance()

	smallerBallot := data.NewBallot(2, 2, inst.replica.Id)
	largerBallot := data.NewBallot(2, 4, inst.replica.Id)

	inst.ballot = largerBallot

	ac := &data.Accept{
		Ballot: smallerBallot,
	}

	// expect:
	// - action: noAction
	// - message: nil
	action, m := inst.nilStatusProcess(ac)
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
}

// This function asserts that one instance will handle an accept
// message if the ballot of the message is larger.
func TestNilStatusProcessWithHandleAccept(t *testing.T) {
	inst := commonTestlibExampleNilStatusInstance()

	smallerBallot := data.NewBallot(2, 2, inst.replica.Id)
	largerBallot := data.NewBallot(2, 4, inst.replica.Id)

	inst.ballot = smallerBallot

	expectedDeps := data.Dependencies{5, 0, 0, 0, 0}
	expectedCmds := commonTestlibExampleCommands()

	ac := &data.Accept{
		Cmds:   expectedCmds,
		Deps:   expectedDeps,
		Ballot: largerBallot,
	}

	action, m := inst.nilStatusProcess(ac)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with, Ballot = largerBallot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.AcceptReply{
		ReplicaId:  inst.rowId,
		InstanceId: inst.id,
		Ballot:     largerBallot,
	})
}

// This function asserts that one instance will handle a commit message
func TestNilStatusProcessWithHandleCommit(t *testing.T) {
	inst := commonTestlibExampleNilStatusInstance()

	instBallot := data.NewBallot(2, 4, inst.replica.Id)

	inst.ballot = instBallot

	expectedDeps := data.Dependencies{5, 0, 0, 0, 0}
	expectedCmds := commonTestlibExampleCommands()

	cm := &data.Commit{
		Cmds: expectedCmds,
		Deps: expectedDeps,
	}

	action, m := inst.nilStatusProcess(cm)

	// expect:
	// - action: noAction
	// - message: nil
	assert.Equal(t, action, noAction)
	assert.Nil(t, m)
}

// This function asserts that one instance will ignore a prepare
// message if the ballot of the message is smaller.
func TestNilStatusProcessWithIgnorePrepare(t *testing.T) {
	inst := commonTestlibExampleNilStatusInstance()

	smallerBallot := data.NewBallot(2, 2, inst.replica.Id)
	largerBallot := data.NewBallot(2, 4, inst.replica.Id)

	inst.ballot = largerBallot

	p := &data.Prepare{
		Ballot: smallerBallot,
	}

	action, m := inst.nilStatusProcess(p)

	// expect:
	// - action: noAction
	// - message: nil
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
}

// This function asserts that one instance will handle a prepare message
// if the ballot of the message is larger.
func TestNilStatusProcessWithHandlePrepare(t *testing.T) {
	inst := commonTestlibExampleNilStatusInstance()

	smallerBallot := data.NewBallot(2, 2, inst.replica.Id)
	largerBallot := data.NewBallot(2, 4, inst.replica.Id)

	expectedDeps := data.Dependencies{5, 0, 0, 0, 0}
	expectedCmds := commonTestlibExampleCommands()

	inst.cmds = expectedCmds
	inst.deps = expectedDeps
	inst.ballot = smallerBallot

	p := &data.Prepare{
		ReplicaId:  inst.rowId,
		InstanceId: inst.id,
		Ballot:     largerBallot,
	}

	action, m := inst.nilStatusProcess(p)

	// expect:
	// - action: replyAction
	// - message: PrepareReply with
	//   Ballot = largerBallot,
	//   OriginalBallot = smallBallot,
	//   Status = nilStatus
	//   IsFromLeader = true,
	//   others are the same as in the instance
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.PrepareReply{
		ReplicaId:      inst.rowId,
		InstanceId:     inst.id,
		Status:         nilStatus,
		Cmds:           expectedCmds,
		Deps:           expectedDeps,
		Ballot:         largerBallot,
		OriginalBallot: smallerBallot,
		IsFromLeader:   false,
	})
}

// This function asserts that one instance will ignore a prepare-reply message
// if the instance is not at initial round, which means it must have been reverted from
// preparing, so the prepare-reply message is stale.
func TestNilStatusProcessWithIgnorePrepareReply(t *testing.T) {
	inst := commonTestlibExampleNilStatusInstance()

	instBallot := data.NewBallot(2, 2, inst.replica.Id)
	inst.ballot = instBallot

	pr := &data.PrepareReply{}
	action, m := inst.nilStatusProcess(pr)

	// expect:
	// - action: noAction
	// - message: nil
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
}

// This function asserts that one instance will panic
// if it receives prepare-reply, pre-accept-reply, accept-reply and pre-accept-ok
// at its initial round, since it could not have sent out such requests
func TestNilStatusProcessWithPanicOnReplies(t *testing.T) {
	inst := commonTestlibExampleNilStatusInstance()

	assert.Panics(t, func() { inst.nilStatusProcess(&data.PrepareReply{}) })
	assert.Panics(t, func() { inst.nilStatusProcess(&data.PreAcceptReply{}) })
	assert.Panics(t, func() { inst.nilStatusProcess(&data.AcceptReply{}) })
	assert.Panics(t, func() { inst.nilStatusProcess(&data.PreAcceptOk{}) })
}

// ************************
// ****** PREACCEPTED *****
// ************************

// TestPreAcceptedProcessWithIgnorePreAccept asserts that
// On receiving smaller ballot pre-accept, preAccepted instance will ignore it.
func TestPreAcceptedProcessWithIgnorePreAccept(t *testing.T) {
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
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, reply, nil)
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithHandlePreAccept asserts that
// On receiving larger ballot pre-accept, preaccepted instance will handle and
// reply with the correct deps
func TestPreAcceptedProcessWithHandlePreAccept(t *testing.T) {
	inst := commonTestlibExamplePreAcceptedInstance()

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	expectedDeps := data.Dependencies{5, 0, 0, 0, 0}
	expectedCmds := commonTestlibExampleCommands()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.cmds = expectedCmds
	expectedInst.deps = expectedDeps
	expectedInst.ballot = largerBallot

	// This is the pre-accept with larger ballot than instance.
	// instance should handle it.
	p := &data.PreAccept{
		Cmds:   expectedCmds,
		Deps:   expectedDeps,
		Ballot: largerBallot,
	}
	action, reply := inst.preAcceptedProcess(p)

	// expect:
	// - action: replyAction
	// - message: PreAcceptReply with ok == true, ballot == largerBallot
	// - instance: ballot == largeBallot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, reply, &data.PreAcceptReply{
		ReplicaId:  inst.rowId,
		InstanceId: inst.id,
		Deps:       expectedDeps,
		Ballot:     largerBallot,
	})
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithIgnoreAccept asserts that
// On receiving smaller ballot accept, preAccepted instance will ignore it.
func TestPreAcceptedProcessWithIgnoreAccept(t *testing.T) {
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
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, reply, nil)
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

	// create expected cmds and deps
	expectedCmds := commonTestlibExampleCommands()
	expectedDeps := commonTestlibExampleDeps()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.cmds = expectedCmds
	expectedInst.deps = expectedDeps
	expectedInst.status = accepted

	// create and send an accept message to the instance
	ac := &data.Accept{
		Cmds:   expectedCmds,
		Deps:   expectedDeps,
		Ballot: smallerBallot,
	}
	action, reply := inst.preAcceptedProcess(ac)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == true, ballot == largerBallot
	// - instance: cmds, deps, ballot are changed, and status == accepted
	assert.Equal(t, action, replyAction)
	assert.Equal(t, reply, &data.AcceptReply{
		Ballot:     smallerBallot,
		ReplicaId:  inst.rowId,
		InstanceId: inst.id,
	})
	expectedInst.ballot = smallerBallot
	assert.Equal(t, inst, expectedInst)

	// test larger ballot accept
	// the above one is test both in same ballot. here the accept ballot is larger.
	inst = commonTestlibExamplePreAcceptedInstance()
	expectedInst = commonTestlibCloneInstance(inst)
	expectedInst.cmds = expectedCmds
	expectedInst.deps = expectedDeps
	expectedInst.status = accepted

	inst.status = preAccepted
	ac.Ballot = largerBallot
	_, reply = inst.preAcceptedProcess(ac)
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

	// create expected cmds and deps
	expectedCmds := commonTestlibExampleCommands()
	expectedDeps := commonTestlibExampleDeps()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.cmds = expectedCmds
	expectedInst.deps = expectedDeps
	expectedInst.status = committed

	// create and send a commit message to the instance
	cm := &data.Commit{
		Cmds: expectedCmds,
		Deps: expectedDeps,
	}
	action, m := inst.preAcceptedProcess(cm)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: cmds and deps are changed, and status == committed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithIgnorePrepare asserts that
// On receiving smaller ballot prepare, preaccepted instance will ignore it.
func TestPreAcceptedProcessWithIgnorePrepare(t *testing.T) {
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
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
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
	//            cmds, deps == inst.cmds, inst.deps
	//            original ballot = smallerBallot
	// - instance: inst.ballot = largerBallot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.PrepareReply{
		IsFromLeader:   false,
		ReplicaId:      inst.rowId,
		InstanceId:     inst.id,
		Status:         preAccepted,
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

	expectedDeps := commonTestlibUnionedDeps()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.status = accepted
	expectedInst.deps = expectedDeps

	inst.info.preAcceptReplyCount = inst.replica.quorum() - 1

	// create and send a prepare message to the instance
	pr := &data.PreAcceptReply{
		Ballot: inst.ballot,
		Deps:   commonTestlibExampleDeps(),
	}
	action, m := inst.preAcceptedProcess(pr)

	// expect:
	// - action: broadcastAction
	// - message: accept with correct cmds, deps
	// - instance: status == accepted
	assert.Equal(t, action, broadcastAction)
	assert.Equal(t, m, &data.Accept{
		Cmds:       inst.cmds,
		Deps:       expectedDeps,
		ReplicaId:  inst.rowId,
		InstanceId: inst.id,
		Ballot:     inst.ballot,
	})
	assert.Equal(t, inst, expectedInst)
}

// **********************
// **** FAST PATH *******
// **********************

// preaccepted instance should go fast path,
// - on receiving fast quorum of preaccept-ok;
func TestPreAcceptedFastPath(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()
	// shold be initial round
	assert.True(t, i.ballot.IsInitialBallot())

	reply := &data.PreAcceptOk{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
	}

	for count := 0; count < i.replica.fastQuorum(); count++ {
		action, msg := i.preAcceptedProcess(reply)
		if count != i.replica.fastQuorum()-1 {
			assert.Equal(t, i.status, preAccepted)
			assert.Equal(t, i.info.preAcceptOkCount, count+1)
			assert.True(t, i.info.samePreAcceptReplies)
			assert.Equal(t, action, noAction)
		} else {
			assert.Equal(t, action, broadcastAction)
			assert.Equal(t, i.status, committed)
			assert.Equal(t, msg, &data.Commit{
				ReplicaId:  i.rowId,
				InstanceId: i.id,
				Cmds:       i.cmds,
				Deps:       i.deps,
			})
		}
	}
}

// - on receiving fast quorum of identical preaccept-reply;
func TestPreAcceptedFastPath2(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()
	// shold be initial round
	assert.True(t, i.ballot.IsInitialBallot())

	newerDeps := i.deps
	newerDeps[i.rowId+1]++

	reply := &data.PreAcceptReply{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Deps:       newerDeps,
		Ballot:     i.ballot,
	}

	for count := 0; count < i.replica.fastQuorum(); count++ {
		action, msg := i.preAcceptedProcess(reply)
		if count != i.replica.fastQuorum()-1 {
			assert.Equal(t, i.status, preAccepted)
			assert.Equal(t, i.info.preAcceptReplyCount, count+1)
			assert.True(t, i.info.samePreAcceptReplies)
			assert.Equal(t, i.deps, newerDeps)
			assert.Equal(t, action, noAction)
		} else {
			p := msg.(*data.Commit)
			assert.Equal(t, action, broadcastAction)
			assert.Equal(t, i.status, committed)
			assert.Equal(t, p.Deps, newerDeps)
		}
	}
}

// **********************
// **** SLOW PATH *******
// **********************

// preaccepted instance should go slow path,
// - on receiving different pre-accept replies.
func TestPreAcceptedSlowPath(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()

	reply := &data.PreAcceptReply{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Deps:       commonTestlibExampleDeps(),
		Ballot:     i.ballot,
	}

	// the deps from replies is not the same as the unioned one. test purpose only
	for count := 0; count < i.replica.quorum(); count++ {
		action, msg := i.preAcceptedProcess(reply)
		if count != i.replica.quorum()-1 {
			assert.Equal(t, i.status, preAccepted)
			assert.Equal(t, i.deps, commonTestlibUnionedDeps())
			assert.Equal(t, action, noAction)
		} else {
			assert.Equal(t, action, broadcastAction)
			assert.Equal(t, i.status, accepted)
			ac := msg.(*data.Accept)
			assert.Equal(t, ac.Deps, commonTestlibUnionedDeps())
		}
	}
}

// - on receiving a mix of preaccept-ok/-reply messages
func TestPreAcceptedSlowPath2(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()

	newerDeps := i.deps
	newerDeps[i.rowId+1]++

	okReply := &data.PreAcceptOk{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
	}
	reply := &data.PreAcceptReply{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Deps:       newerDeps,
		Ballot:     i.ballot,
	}

	// the deps from replies is not the same as the unioned one. test purpose only
	for count := 0; count < i.replica.quorum(); count++ {
		if count != i.replica.quorum()-1 {
			action, _ := i.preAcceptedProcess(reply)
			assert.Equal(t, i.status, preAccepted)
			assert.Equal(t, i.deps, newerDeps)
			assert.True(t, i.info.samePreAcceptReplies)
			assert.Equal(t, action, noAction)
		} else {
			action, msg := i.preAcceptedProcess(okReply)
			assert.Equal(t, action, broadcastAction)
			assert.Equal(t, i.status, accepted)
			ac := msg.(*data.Accept)
			assert.Equal(t, ac.Deps, newerDeps)
		}
	}
}

// TestPreAcceptedProcessWithIgnorePreAcceptOk asserts that
// when a pre-accepted instance receives a pre-accept-ok message, it
// will ignore the message if the instance is not at initial round.
func TestPreAcceptedProcessWithIgnorePreAcceptOk(t *testing.T) {
	// create a pre-accepted instance
	inst := commonTestlibExamplePreAcceptedInstance()

	inst.ballot = data.NewBallot(2, 2, inst.replica.Id)
	expectedInst := commonTestlibCloneInstance(inst)

	// create and send a prepare message to the instance
	pr := &data.PreAcceptOk{}
	action, m := inst.preAcceptedProcess(pr)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Nil(t, m)
	assert.Equal(t, inst, expectedInst)
}

// TestPreAcceptedProcessWithHandlePreAcceptOk asserts that
// when a pre-accepted instance receives a pre-accept-ok message, it
// will handle the message if the instance is at initial round.
func TestPreAcceptedProcessWithHandlePreAcceptOk(t *testing.T) {
	// create a pre-accepted instance
	i := commonTestlibExamplePreAcceptedInstance()

	expectedInst := commonTestlibCloneInstance(i)
	expectedInst.status = committed
	expectedInst.info.preAcceptOkCount = i.replica.fastQuorum()

	i.info.preAcceptOkCount = i.replica.fastQuorum() - 1

	// create and send a prepare message to the instance
	pr := &data.PreAcceptOk{}
	action, m := i.preAcceptedProcess(pr)

	// expect:
	// - action: broadcastAction
	// - message: accept with correct cmds, deps
	// - instance: status == accepted
	assert.Equal(t, action, broadcastAction)
	assert.Equal(t, m, &data.Commit{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       i.cmds,
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

// TestAcceptedProcessWithIgnorePreAccept asserts that
// when an accepted instance receives preaccept, it should ignore it.
func TestAcceptedProcessWithIgnorePreAccept(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	expectedInst := commonTestlibCloneInstance(inst)

	// send a pre-accept message to it
	pa := &data.PreAccept{}
	action, m := inst.acceptedProcess(pa)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
	assert.Equal(t, inst, expectedInst)
}

// TestAcceptedProcessWithIgnoreAccept asserts that
// on receiving smaller ballot accept, accepted instance will ignore it.
func TestAcceptedProcessWithIgnoreAccept(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = largerBallot
	expectedInst := commonTestlibCloneInstance(inst)

	// create an Accept message with small ballot, and send it to the instance
	ac := &data.Accept{
		Ballot: smallerBallot,
	}
	action, m := inst.acceptedProcess(ac)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
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
	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.cmds = cmds
	expectedInst.deps = deps
	expectedInst.status = accepted
	expectedInst.ballot = largeBallot

	ac := &data.Accept{
		Cmds:   cmds,
		Deps:   deps,
		Ballot: largeBallot,
	}
	action, m := inst.acceptedProcess(ac)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == true, ballot = inst.ballot
	// - instace:
	//     cmds = accept.cmds,
	//     deps = accept.deps,
	//     ballot = accept.ballot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.AcceptReply{
		ReplicaId:  inst.rowId,
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

	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.cmds = cmds
	expectedInst.deps = deps
	expectedInst.status = committed

	// create a commit message and send it to the instance
	cm := &data.Commit{
		Cmds:       cmds,
		Deps:       deps,
		ReplicaId:  inst.rowId,
		InstanceId: inst.id,
	}
	action, m := inst.acceptedProcess(cm)

	// expect:
	// - action: noAction
	// - msg: nil
	// - instance: cmds == commit.cmds, deps == commit.deps
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
	assert.Equal(t, inst, expectedInst)
}

// TestAcceptedProcessWithIgnorePrepare asserts that
// when an accepted instance receives prepare, it should ignore the message if
// the ballot of the prepare message is larger than that of the instance.
func TestAcceptedProcessWithIgnorePrepare(t *testing.T) {
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
	// - action: noAction
	// - msg: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, msg, nil)
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
	// - msg: PrepareReply with ok == true, cmds == inst.cmds,
	//        deps == inst.deps, ballot == largeballot, originalballot == smallballot
	// - instance: ballot = largeballot
	assert.Equal(t, action, replyAction)
	assert.Equal(t, msg, &data.PrepareReply{
		IsFromLeader:   false,
		ReplicaId:      inst.rowId,
		InstanceId:     inst.id,
		Status:         accepted,
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
	inst.info.acceptReplyCount = inst.replica.quorum() - 1

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.info.acceptReplyCount = inst.replica.quorum()
	expectedInst.status = committed

	// create an accept-reply message and send it to the instance
	ar := &data.AcceptReply{
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
		Deps:       inst.deps,
		ReplicaId:  inst.rowId,
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
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Nil(t, m)
	assert.Equal(t, inst, expectedInst)
}

// If a committed instance receives accept, it will ignore the message.
func TestCommittedProcessWithRejcetAccept(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	expectedInst := commonTestlibCloneInstance(inst)
	// send an Accept message to it
	a := &data.Accept{}
	action, m := inst.committedProcess(a)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
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
		ReplicaId:    inst.rowId,
		IsFromLeader: false,
		InstanceId:   inst.id,
		Status:       committed,
		Cmds:         inst.cmds,
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

// committed instance should ignore pre-accept messages.
func TestCommittedProcessWithIgnorePreAccept(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	expectedInst := commonTestlibCloneInstance(inst)

	// send a PreAccept message to it
	p := &data.PreAccept{}
	action, m := inst.committedProcess(p)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
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

// This function asserts that a preparing instance will ignore a
// pre-accept message if the ballot of the message is smaller.
func TestPreparingProcessWithIgnorePreAccept(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = largerBallot
	expectInst := commonTestlibCloneInstance(inst)

	pa := &data.PreAccept{
		Ballot: smallerBallot,
	}
	action, m := inst.preparingProcess(pa)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
	assert.Equal(t, inst, expectInst)
}

// This function asserts that a preparing instance will handle a
// pre-accept message if the ballot of the message is larger.
func TestPreparingProcessWithHandlePreAccept(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	expectedDeps := data.Dependencies{5, 0, 0, 0, 0}
	expectedCmds := commonTestlibExampleCommands()

	inst.ballot = smallerBallot

	pa := &data.PreAccept{
		Cmds:   expectedCmds,
		Deps:   expectedDeps,
		Ballot: largerBallot,
	}
	action, m := inst.preparingProcess(pa)

	// expect:
	// - action: replyAction
	// - message: preAcceptReply with
	//   Ballot == largerBallot,
	//   ohter fields are equal to the instance
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.PreAcceptReply{
		ReplicaId:  inst.rowId,
		InstanceId: inst.id,
		Deps:       expectedDeps,
		Ballot:     largerBallot,
	})
}

// This function asserts that a preparing instance will ignore an
// accept message if the ballot of the message is smaller.
func TestPreparingProcessWithIgnoreAccept(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = largerBallot

	ac := &data.Accept{
		Ballot: smallerBallot,
	}
	action, m := inst.preparingProcess(ac)

	// expect:
	// - action: noAction
	// - message: nil
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
}

// This function asserts that a preparing instance will handle an
// accept message if the ballot of the message is larger.
func TestPreparingProcessWithHandleAccept(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = smallerBallot

	ac := &data.Accept{
		Ballot: largerBallot,
	}
	action, m := inst.preparingProcess(ac)

	// expect:
	// - action: replyAction
	// - message: AcceptReply with
	//   Ballot == largerBallot,
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.AcceptReply{
		ReplicaId:  inst.rowId,
		InstanceId: inst.id,
		Ballot:     largerBallot,
	})
}

// This function asserts that a preparing instance will always handle a
// commit message.
func TestPreparingProcessWithHandleCommit(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	smallerBallot := inst.replica.makeInitialBallot()

	expectedDeps := data.Dependencies{5, 0, 0, 0, 0}
	expectedCmds := commonTestlibExampleCommands()

	inst.ballot = smallerBallot

	expectedInst := commonTestlibCloneInstance(inst)
	expectedInst.deps = expectedDeps
	expectedInst.cmds = expectedCmds
	expectedInst.status = committed

	cm := &data.Commit{
		Cmds: expectedCmds,
		Deps: expectedDeps,
	}
	action, m := inst.preparingProcess(cm)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance:
	//   status = true
	//   Ballot == largerBallot,
	//   other fields are expected
	assert.Equal(t, action, noAction)
	assert.Nil(t, m)
	assert.Equal(t, inst, expectedInst)
}

// This function asserts that a preparing instance will ignore a
// prepare message if the ballot of the message is smaller.
func TestPreparingProcessWithIgnorePrepare(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = largerBallot
	expectedInst := commonTestlibCloneInstance(inst)

	p := &data.Prepare{
		Ballot: smallerBallot,
	}
	action, m := inst.preparingProcess(p)

	// expect:
	// - action: noAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
	assert.Equal(t, inst, expectedInst)
}

// This function asserts that a preparing instance will panic if
// it receives a prepare message with the same ballot
// because that should not happen
func TestPreparingProcessWithPanicPrepare(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	instBallot := inst.replica.makeInitialBallot().IncNumClone()

	inst.ballot = instBallot

	p := &data.Prepare{
		Ballot: instBallot,
	}
	assert.Panics(t, func() { inst.preparingProcess(p) })
}

// This function asserts that a preparing instance will handle
// a prepare message if the message has a larger ballot
func TestPreparingProcessWithHandlePrepare(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.recoveryInfo.formerBallot = smallerBallot
	inst.recoveryInfo.formerStatus = nilStatus
	inst.ballot = smallerBallot

	p := &data.Prepare{
		ReplicaId: inst.rowId,
		Ballot:    largerBallot,
	}
	action, m := inst.preparingProcess(p)

	// expect:
	// - action: replyAction
	// - message: PrepareReply with Ok == true, Ballot = largerBallot,
	//            OriginalBallot == smallerBallot, other fields are
	//            equal to the instance
	assert.Equal(t, action, replyAction)
	assert.Equal(t, m, &data.PrepareReply{
		Status:         nilStatus,
		Ballot:         largerBallot,
		OriginalBallot: smallerBallot,
		ReplicaId:      inst.rowId,
		InstanceId:     inst.id,
		Cmds:           inst.cmds,
		Deps:           inst.deps,
		IsFromLeader:   false,
	})
}

// This function asserts that a preparing instance will ignore
// a prepare-reply message if the message has a smaller ballot
func TestPreparingProcessWithIgnorePrepareReply(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	smallerBallot := inst.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	inst.ballot = largerBallot

	p := &data.PrepareReply{
		Ballot: smallerBallot,
	}
	action, m := inst.preparingProcess(p)

	// expect:
	// - action: noAction
	// - message: nil
	assert.Equal(t, action, noAction)
	assert.Nil(t, m)
}

// This function asserts that a preparing instance will handle
// a prepare-reply message if the message has an equal ballot
func TestPreparingProcessWithHandlePrepareReply(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	instBallot := inst.replica.makeInitialBallot()

	expectedDeps := data.Dependencies{5, 0, 0, 0, 0}
	expectedCmds := commonTestlibExampleCommands()

	inst.ballot = instBallot
	inst.recoveryInfo.replyCount = inst.replica.quorum() - 1

	p := &data.PrepareReply{
		Cmds:           expectedCmds,
		Deps:           expectedDeps,
		Status:         committed,
		Ballot:         instBallot,
		OriginalBallot: instBallot,
	}
	action, m := inst.preparingProcess(p)

	// expect:
	// - action: broadcastAction
	// - message: Commit
	assert.Equal(t, action, broadcastAction)
	assert.Equal(t, m, &data.Commit{
		ReplicaId:  inst.rowId,
		InstanceId: inst.id,
		Cmds:       inst.cmds,
		Deps:       inst.deps,
	})
}

// This function asserts that a preparing instance will ignore
// pre-accept-reply, pre-accept-ok, accept-reply if they are stale.
func TestPreparingProcessWithIgnoreOtherReplies(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	inst.recoveryInfo.formerStatus = accepted

	pr := &data.PreAcceptReply{}
	po := &data.PreAcceptOk{}
	ar := &data.AcceptReply{}

	action, m := inst.preparingProcess(pr)
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)

	action, m = inst.preparingProcess(po)
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)

	action, m = inst.preparingProcess(ar)
	assert.Equal(t, action, noAction)
	assert.Equal(t, m, nil)
}

// This function asserts that a preparing instance will panic
// when
// 1, receiving pre-accept-reply, pre-accept-ok and accept-reply
// if the instance has never entered that status before.
// 2, receiving proposal
// 3, the instance is not at preparing status
func TestPreparingProcessWithPanic(t *testing.T) {
	inst := commonTestlibExamplePreparingInstance()

	inst.recoveryInfo.formerStatus = nilStatus

	pr := &data.PreAcceptReply{}
	po := &data.PreAcceptOk{}
	ar := &data.AcceptReply{}
	pp := &data.Propose{}

	assert.Panics(t, func() { inst.preparingProcess(pr) })
	assert.Panics(t, func() { inst.preparingProcess(po) })
	assert.Panics(t, func() { inst.preparingProcess(ar) })
	assert.Panics(t, func() { inst.preparingProcess(pp) })

	inst.recoveryInfo.formerStatus = accepted
	inst.status = accepted
	assert.Panics(t, func() { inst.preparingProcess(ar) })
}

// If a preparing instance as nilstatus handles
// - committed reply, it should set its recovery info according to the reply
// - accepted reply, it should set its recovery info according to the reply
// - pre-accepted reply, it should set its recovery info according to the reply
// - nilstatus reply, ignore
func TestNilStatusPreparingHandlePrepareReply(t *testing.T) {
	// committed reply
	i := commonTestlibExamplePreparingInstance()
	ir := i.recoveryInfo

	originalBallot := i.replica.makeInitialBallot()
	messageBallot := i.ballot.Clone()

	p := &data.PrepareReply{
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Cmds:           commonTestlibExampleCommands(),
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
	i = commonTestlibExamplePreparingInstance()
	ir = i.recoveryInfo
	p.Status = accepted

	i.handlePrepareReply(p)
	assert.Equal(t, ir.status, accepted)

	// pre-accepted reply
	i = commonTestlibExamplePreparingInstance()
	ir = i.recoveryInfo
	p.Status = preAccepted

	i.handlePrepareReply(p)
	assert.Equal(t, ir.status, preAccepted)

	// nilstatus reply
	i = commonTestlibExamplePreparingInstance()
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
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Cmds:           commonTestlibExampleCommands(),
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
	i.rowId = i.replica.Id
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
	assert.Equal(t, ir.identicalCount, 1)

	i = commonTestlibExamplePreAcceptedInstance()
	i.rowId = i.replica.Id
	i.enterPreparing()
	ir = i.recoveryInfo
	p.Cmds, p.Deps, p.OriginalBallot = ir.cmds, ir.deps, ir.ballot
	p.IsFromLeader = true // from leader
	i.handlePrepareReply(p)
	assert.Equal(t, ir.identicalCount, 0)

	// receiving N/2 identical initial, broadcast accepts
	i = commonTestlibExamplePreAcceptedInstance()
	i.rowId = i.replica.Id
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

// ******************************
// ******* HANDLE MESSAGE *******
// ******************************

// TestHandlePropose tests the correctness of handlePropose
func TestHandlePropose(t *testing.T) {
	i := commonTestlibExampleNilStatusInstance()
	i.replica.Id = i.rowId

	cmds := commonTestlibExampleCommands()
	p := &data.Propose{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       nil,
	}
	// should panic is cmds == nil
	assert.Panics(t, func() { i.handlePropose(p) })

	// should panic if the instance not at nilStatus
	i = commonTestlibExamplePreAcceptedInstance()
	p.Cmds = cmds
	assert.Panics(t, func() { i.handlePropose(p) })

	// should panic if the instance is not at first round
	i = commonTestlibExampleNilStatusInstance()
	i.ballot = i.replica.makeInitialBallot().IncNumClone()
	assert.Panics(t, func() { i.handlePropose(p) })

	i = commonTestlibExampleNilStatusInstance()
	i.replica.Id = i.rowId

	act, msg := i.handlePropose(p)
	assert.Equal(t, act, fastQuorumAction)
	assert.Equal(t, msg, &data.PreAccept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       i.cmds,
		Deps:       i.deps,
		Ballot:     i.ballot,
	})
}

// TestHandlePreAccept tests the correctness of handlePreAccept
func TestHandlePreAccept(t *testing.T) {
	i := commonTestlibExampleNilStatusInstance()

	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()
	deps := commonTestlibExampleDeps()

	i.ballot = largerBallot
	p := &data.PreAccept{
		Cmds:       commonTestlibExampleCommands(),
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Deps:       deps,
		Ballot:     smallerBallot,
	}

	// should panic if the ballot of the pre-accept is smaller
	assert.Panics(t, func() { i.handlePreAccept(p) })

	i.ballot = smallerBallot
	p.Ballot = smallerBallot

	// should reply PreAcceptOk
	act, msg := i.handlePreAccept(p)
	assert.Equal(t, act, replyAction)
	assert.Equal(t, msg, &data.PreAcceptOk{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
	})

	deps = data.Dependencies{1, 2, 3, 4, 5}
	p.Deps = deps.Clone()

	// make instance[1][9] conflict with the pre-accept
	i.replica.MaxInstanceNum[i.rowId+1] = 10
	i.replica.InstanceMatrix[i.rowId+1][9] = commonTestlibCloneInstance(i)
	expectedDeps := data.Dependencies{1, 2, 9, 4, 5}

	act, msg = i.handlePreAccept(p)

	// should have Deps
	assert.Equal(t, act, replyAction)
	assert.Equal(t, msg, &data.PreAcceptReply{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Deps:       expectedDeps,
		Ballot:     smallerBallot,
	})

	// make the pre-accept not in the initial round
	p.Ballot = largerBallot
	p.Deps = deps.Clone()
	act, msg = i.handlePreAccept(p)
	assert.Equal(t, act, replyAction)
	assert.Equal(t, msg, &data.PreAcceptReply{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Deps:       expectedDeps,
		Ballot:     largerBallot,
	})
}

// TestHandlePreAcceptOk tests the correctness of handlePreAcceptOk
func TestHandlePreAcceptOk(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()
	p := &data.PreAcceptOk{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
	}

	initialBallot := i.replica.makeInitialBallot()
	largeBallot := initialBallot.IncNumClone()

	// should panic if i is not at initial round
	i.ballot = largeBallot
	assert.Panics(t, func() { i.handlePreAcceptOk(p) })

	i.ballot = initialBallot

	// should receive noAction and nil message since
	// we haven't got enough replies
	act, msg := i.handlePreAcceptOk(p)
	assert.Equal(t, act, noAction)
	assert.Equal(t, msg, nil)
	assert.Equal(t, i.info.preAcceptOkCount, 1)

	// should receive broadcastAction and a commit message
	i.info.preAcceptOkCount = i.replica.fastQuorum() - 1
	act, msg = i.handlePreAcceptOk(p)
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Commit{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       i.cmds,
		Deps:       i.deps,
	})
	assert.Equal(t, i.status, committed)

	// should panic since we should have changed to other state
	assert.Panics(t, func() { i.handlePreAcceptOk(p) })
}

// This func tests the correctness of handlePreAcceptReply
func TestHandlePreAcceptReply(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()
	p := &data.PreAcceptReply{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
	}
	smallBallot := i.replica.makeInitialBallot()
	largeBallot := smallBallot.IncNumClone()

	i.ballot = largeBallot
	p.Ballot = smallBallot

	// should panic
	assert.Panics(t, func() { i.handlePreAcceptReply(p) })

	// should panic too
	i.ballot = smallBallot
	p.Ballot = largeBallot
	assert.Panics(t, func() { i.handlePreAcceptReply(p) })

	i.ballot = smallBallot
	p.Ballot = smallBallot
	p.Deps = commonTestlibExampleDeps()

	// receive the first reply, nothing should happens
	act, msg := i.handlePreAcceptReply(p)
	assert.Equal(t, act, noAction)
	assert.Equal(t, msg, nil)
	assert.Equal(t, i.info.samePreAcceptReplies, true)
	assert.Equal(t, i.info.preAcceptReplyCount, 1)

	// receive the second not same reply,
	// i should send out accept message (since total replica size is 5)
	i = commonTestlibExamplePreAcceptedInstance()
	i.ballot = smallBallot
	p.Ballot = smallBallot
	i.info.preAcceptReplyCount = 1
	act, msg = i.handlePreAcceptReply(p)

	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Accept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     smallBallot,
		Deps:       i.deps,
		Cmds:       i.cmds,
	})
}

// TestHandleAccept tests the correctness of handlePreAcceptOk
func TestHandleAccept(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()
	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()
	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	i.ballot = largerBallot

	a := &data.Accept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       cmds,
		Deps:       deps,
		Ballot:     smallerBallot,
	}

	// should panic if the instance has a larger ballot
	assert.Panics(t, func() { i.handleAccept(a) })

	// should panic if the instance is already committed
	i.ballot = smallerBallot
	i.status = committed
	assert.Panics(t, func() { i.handleAccept(a) })

	// test response
	i.status = preAccepted
	a.Ballot = largerBallot
	act, msg := i.handleAccept(a)

	assert.Equal(t, act, replyAction)
	assert.Equal(t, msg, &data.AcceptReply{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     largerBallot,
	})
}

// TestHandleAcceptReply tests the correctness of the handleAcceptReply
func TestHandleAcceptReply(t *testing.T) {
	i := commonTestlibExampleAcceptedInstance()
	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()

	i.cmds = cmds.Clone()
	i.deps = deps.Clone()

	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	a := &data.AcceptReply{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     smallerBallot,
	}
	i.ballot = largerBallot

	// should panic if the instance has a larger ballot
	assert.Panics(t, func() { i.handleAcceptReply(a) })

	// should panic if the reply has a larger ballot
	a.Ballot = largerBallot
	i.ballot = smallerBallot
	assert.Panics(t, func() { i.handleAcceptReply(a) })

	a.Ballot = smallerBallot
	i.ballot = smallerBallot

	i.info.acceptReplyCount = i.replica.quorum() - 2
	act, msg := i.handleAcceptReply(a)

	// should be noAction and nil message, since i hasn't
	// received enough replies yet
	assert.Equal(t, act, noAction)
	assert.Equal(t, msg, nil)
	assert.Equal(t, i.info.acceptReplyCount, i.replica.quorum()-1)

	// now i has received enough replies,
	// it should return broadcastAction and a commit message
	act, msg = i.handleAcceptReply(a)
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Commit{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       cmds,
		Deps:       deps,
	})

	// should panic since i should call handleAcceptReply after it
	// changes to committed status
	assert.Panics(t, func() { i.handleAcceptReply(a) })
}

// It's testing `handleprepare` will return (replyaction, correct prepare-reply)
func TestHandlePrepare(t *testing.T) {
	i := commonTestlibExamplePreAcceptedInstance()

	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	i.rowId = i.replica.Id
	i.ballot = smallerBallot
	i.deps = data.Dependencies{3, 4, 5, 6, 7}

	prepare := &data.Prepare{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
	}

	prepare.Ballot = smallerBallot
	assert.Panics(t, func() { i.handlePrepare(prepare) })

	prepare.Ballot = largerBallot
	action, reply := i.handlePrepare(prepare)

	assert.Equal(t, action, replyAction)
	// it should return {
	//   ok = true, correct status, deps, ballots
	// }
	assert.Equal(t, reply, &data.PrepareReply{
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
		Deps:       commonTestlibExampleDeps(),
	}
	action, msg := i.handleCommit(cm)

	assert.Equal(t, i.status, committed)
	assert.Equal(t, i.cmds, cm.Cmds)
	assert.Equal(t, i.deps, cm.Deps)
	assert.Equal(t, action, noAction)
	assert.Equal(t, msg, nil)

	i = commonTestlibExamplePreAcceptedInstance()
	i.handleCommit(cm)
	assert.Equal(t, i.status, committed)

	i = commonTestlibExamplePreparingInstance()
	i.handleCommit(cm)
	assert.Equal(t, i.status, committed)

	i = commonTestlibExampleNilStatusInstance()
	i.handleCommit(cm)
	assert.Equal(t, i.status, committed)

	i = commonTestlibExampleCommittedInstance()
	assert.Panics(t, func() { i.handleCommit(cm) })
}

// This func tests the correctness of handlePrepareReply
func TestHandlePrepareReply(t *testing.T) {
	i := commonTestlibExamplePreparingInstance()

	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()
	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()
	p := &data.PrepareReply{
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Status:         committed,
		Cmds:           cmds,
		Deps:           deps,
		Ballot:         largerBallot,
		OriginalBallot: smallerBallot,
		IsFromLeader:   false,
	}

	// should panic if i is not at prepare status
	i.status = committed
	assert.Panics(t, func() { i.handlePrepareReply(p) })

	// should panic if p has a smaller ballot, which means the reply is stale
	i.status = preparing
	i.ballot = largerBallot
	p.Ballot = smallerBallot
	assert.Panics(t, func() { i.handlePrepareReply(p) })

	// should panic if p has a larger ballot
	i.ballot = smallerBallot
	p.Ballot = largerBallot
	assert.Panics(t, func() { i.handlePrepareReply(p) })

	// should return noAction and nil,
	// and i's ballot should be updated
	i.ballot = largerBallot
	act, msg := i.handlePrepareReply(p)
	assert.Equal(t, act, noAction)
	assert.Equal(t, msg, nil)
	assert.Equal(t, i.ballot, largerBallot)

	// should panic if i have already received enough replies
	i.ballot = smallerBallot
	p.Ballot = smallerBallot
	i.recoveryInfo.replyCount = i.replica.quorum()
	assert.Panics(t, func() { i.handlePrepareReply(p) })

	// should return noAction, nil, since we haven't received enough replies
	i = commonTestlibExamplePreparingInstance()
	i.ballot = smallerBallot
	i.recoveryInfo.replyCount = i.replica.quorum() - 2
	p.Status = accepted
	act, msg = i.handlePrepareReply(p)
	assert.Equal(t, act, noAction)
	assert.Equal(t, msg, nil)

	// should return broadcastAction, commit
	p.Status = committed
	act, msg = i.handlePrepareReply(p)
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Commit{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       cmds,
		Deps:       deps,
	})
}

// *******************************************
// ***** Tests for prepare-reply helpers *****
// *******************************************

func TestUpdateReocveryInstance(t *testing.T) {
	i := commonTestlibExamplePreparingInstance()
	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()
	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()
	p := &data.PrepareReply{
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Status:         committed,
		Cmds:           cmds,
		Deps:           deps,
		Ballot:         smallerBallot,
		OriginalBallot: largerBallot,
		IsFromLeader:   false,
	}

	// should panic if p has invalid status
	p.Status = preparing
	assert.Panics(t, func() { i.updateRecoveryInstance(p) })

	// go through all the path
	p.Status = committed
	i.updateRecoveryInstance(p)
	p.Status = accepted
	i.updateRecoveryInstance(p)
	p.Status = preAccepted
	i.updateRecoveryInstance(p)
	p.Status = nilStatus
	i.updateRecoveryInstance(p)
}

func TestHandleCommittedPrepareReply(t *testing.T) {
	i := commonTestlibExamplePreparingInstance()
	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	iDeps := i.recoveryInfo.deps.Clone()
	iCmds := i.recoveryInfo.cmds.Clone()
	iBallot := i.recoveryInfo.ballot.Clone()

	pCmds := commonTestlibExampleCommands()
	pDeps := commonTestlibExampleDeps()
	p := &data.PrepareReply{
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Status:         committed,
		Cmds:           pCmds,
		Deps:           pDeps,
		Ballot:         smallerBallot,
		OriginalBallot: largerBallot,
		IsFromLeader:   false,
	}

	// nothing should be changed if i has already received a committed reply
	i.recoveryInfo.status = committed
	i.handleCommittedPrepareReply(p)
	assert.Equal(t, i.recoveryInfo.deps, iDeps)
	assert.Equal(t, i.recoveryInfo.cmds, iCmds)
	assert.Equal(t, i.recoveryInfo.ballot, iBallot)
	assert.Equal(t, i.recoveryInfo.status, committed)

	// should update the recoveryInfo if i has not received a committed reply yet
	i.recoveryInfo.status = nilStatus

	i.handleCommittedPrepareReply(p)
	assert.Equal(t, i.recoveryInfo.deps, pDeps)
	assert.Equal(t, i.recoveryInfo.cmds, pCmds)
	assert.Equal(t, i.recoveryInfo.ballot, largerBallot)
	assert.Equal(t, i.recoveryInfo.status, committed)
}

func TestHandleAcceptedPrepareReply(t *testing.T) {
	i := commonTestlibExamplePreparingInstance()
	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	iDeps := i.recoveryInfo.deps.Clone()
	iCmds := i.recoveryInfo.cmds.Clone()
	iBallot := i.recoveryInfo.ballot.Clone()

	pCmds := commonTestlibExampleCommands()
	pDeps := commonTestlibExampleDeps()
	p := &data.PrepareReply{
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Status:         accepted,
		Cmds:           pCmds,
		Deps:           pDeps,
		Ballot:         largerBallot,
		OriginalBallot: smallerBallot,
		IsFromLeader:   false,
	}
	i.recoveryInfo.status = committed

	// should not change the recoveryInfo if
	// i has received a later status than accepted from the replies
	i.handleAcceptedPrepareReply(p)
	assert.Equal(t, i.recoveryInfo.deps, iDeps)
	assert.Equal(t, i.recoveryInfo.cmds, iCmds)
	assert.Equal(t, i.recoveryInfo.ballot, iBallot)
	assert.Equal(t, i.recoveryInfo.status, committed)

	// should update the recoveryInfo if
	// i has not received a later status than accepted from the replies yet
	i.recoveryInfo.status = preAccepted
	i.handleAcceptedPrepareReply(p)
	assert.Equal(t, i.recoveryInfo.deps, pDeps)
	assert.Equal(t, i.recoveryInfo.cmds, pCmds)
	assert.Equal(t, i.recoveryInfo.ballot, smallerBallot)
	assert.Equal(t, i.recoveryInfo.status, accepted)

	// should not change the recoveryInfo if
	// i has received an accepted-reply with larger ballot
	i = commonTestlibExamplePreparingInstance()
	i.recoveryInfo.status = accepted
	i.recoveryInfo.ballot = largerBallot
	i.handleAcceptedPrepareReply(p)

	assert.Equal(t, i.recoveryInfo.deps, iDeps)
	assert.Equal(t, i.recoveryInfo.cmds, iCmds)
	assert.Equal(t, i.recoveryInfo.ballot, largerBallot)
	assert.Equal(t, i.recoveryInfo.status, accepted)

	// should update the recoveryInfo if
	// p has a larger ballot than all the replies
	// that i has ever received
	i.recoveryInfo.ballot = smallerBallot
	p.OriginalBallot = largerBallot
	i.handleAcceptedPrepareReply(p)

	assert.Equal(t, i.recoveryInfo.deps, pDeps)
	assert.Equal(t, i.recoveryInfo.cmds, pCmds)
	assert.Equal(t, i.recoveryInfo.ballot, largerBallot)
	assert.Equal(t, i.recoveryInfo.status, accepted)
}

func TestHandlePreAcceptedPrepareReply(t *testing.T) {
	i := commonTestlibExamplePreparingInstance()
	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	iDeps := i.recoveryInfo.deps.Clone()
	iCmds := i.recoveryInfo.cmds.Clone()
	iBallot := i.recoveryInfo.ballot.Clone()

	pCmds := commonTestlibExampleCommands()
	pDeps := commonTestlibExampleDeps()
	p := &data.PrepareReply{
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Status:         preAccepted,
		Cmds:           pCmds,
		Deps:           pDeps,
		Ballot:         largerBallot,
		OriginalBallot: smallerBallot,
		IsFromLeader:   false,
	}

	// should not change the recoveryInfo if
	// i has received a later status than pre-accepted from the replies
	i.recoveryInfo.status = accepted
	i.handlePreAcceptedPrepareReply(p)
	assert.Equal(t, i.recoveryInfo.deps, iDeps)
	assert.Equal(t, i.recoveryInfo.cmds, iCmds)
	assert.Equal(t, i.recoveryInfo.ballot, iBallot)
	assert.Equal(t, i.recoveryInfo.status, accepted)

	// should update the recoveryInfo if
	// i has not received a later status than pre-accepted from the replies yet
	i.recoveryInfo.status = nilStatus
	i.handlePreAcceptedPrepareReply(p)
	assert.Equal(t, i.recoveryInfo.deps, pDeps)
	assert.Equal(t, i.recoveryInfo.cmds, pCmds)
	assert.Equal(t, i.recoveryInfo.ballot, smallerBallot)
	assert.Equal(t, i.recoveryInfo.status, preAccepted)
	// identicalCount should be 1 since the reply if from a member of fast quorom
	assert.Equal(t, i.recoveryInfo.identicalCount, 1)

	// should not change the recoveryInfo if
	// i has received an pre-accepted-reply with larger ballot
	i = commonTestlibExamplePreparingInstance()
	i.recoveryInfo.status = preAccepted
	i.recoveryInfo.ballot = largerBallot
	i.handlePreAcceptedPrepareReply(p)

	assert.Equal(t, i.recoveryInfo.deps, iDeps)
	assert.Equal(t, i.recoveryInfo.cmds, iCmds)
	assert.Equal(t, i.recoveryInfo.ballot, largerBallot)
	assert.Equal(t, i.recoveryInfo.status, preAccepted)

	// should update the recoveryInfo if
	// p has a larger ballot than all the replies
	// that i has ever received
	i.recoveryInfo.ballot = smallerBallot
	p.OriginalBallot = largerBallot
	i.handlePreAcceptedPrepareReply(p)

	assert.Equal(t, i.recoveryInfo.deps, pDeps)
	assert.Equal(t, i.recoveryInfo.cmds, pCmds)
	assert.Equal(t, i.recoveryInfo.ballot, largerBallot)
	assert.Equal(t, i.recoveryInfo.status, preAccepted)
	// identicalCount should be zero
	assert.Equal(t, i.recoveryInfo.identicalCount, 0)

	// should increase the identicalCount when receiving
	// an identical reply
	i = commonTestlibExamplePreparingInstance()
	i.recoveryInfo.status = nilStatus
	i.recoveryInfo.ballot = smallerBallot
	p.OriginalBallot = smallerBallot
	i.handlePreAcceptedPrepareReply(p)
	i.handlePreAcceptedPrepareReply(p)

	assert.Equal(t, i.recoveryInfo.deps, pDeps)
	assert.Equal(t, i.recoveryInfo.cmds, pCmds)
	assert.Equal(t, i.recoveryInfo.ballot, smallerBallot)
	assert.Equal(t, i.recoveryInfo.status, preAccepted)
	// identicalCount should be 2
	assert.Equal(t, i.recoveryInfo.identicalCount, 2)
}

func TestMakeRecoveryDecision(t *testing.T) {
	i := commonTestlibExamplePreparingInstance()

	smallerBallot := i.replica.makeInitialBallot()
	largerBallot := smallerBallot.IncNumClone()

	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()

	// should get broadcastAction and commit message
	i.initRecoveryInfo()
	i.recoveryInfo.status = committed
	i.recoveryInfo.cmds = cmds
	i.recoveryInfo.deps = deps
	i.recoveryInfo.ballot = largerBallot

	act, msg := i.makeRecoveryDecision()
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Commit{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       cmds,
		Deps:       deps,
	})

	// should get broadcastAction and accept message
	i = commonTestlibExamplePreparingInstance()
	i.initRecoveryInfo()
	i.recoveryInfo.status = accepted
	i.recoveryInfo.cmds = cmds
	i.recoveryInfo.deps = deps
	i.recoveryInfo.ballot = largerBallot

	act, msg = i.makeRecoveryDecision()
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Accept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       cmds,
		Deps:       deps,
		Ballot:     largerBallot,
	})

	// should get broadcastAction and accept message
	i = commonTestlibExamplePreparingInstance()
	i.initRecoveryInfo()
	i.recoveryInfo.status = preAccepted
	i.recoveryInfo.cmds = cmds
	i.recoveryInfo.deps = deps
	i.recoveryInfo.identicalCount = i.replica.quorum()
	i.recoveryInfo.ballot = largerBallot

	act, msg = i.makeRecoveryDecision()
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Accept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       cmds,
		Deps:       deps,
		Ballot:     largerBallot,
	})

	// should get broadcastAction and pre-accept message
	i = commonTestlibExamplePreparingInstance()
	i.initRecoveryInfo()
	i.recoveryInfo.status = preAccepted
	i.recoveryInfo.cmds = cmds
	i.recoveryInfo.deps = deps
	i.recoveryInfo.identicalCount = i.replica.quorum() - 1
	i.recoveryInfo.ballot = largerBallot

	act, msg = i.makeRecoveryDecision()
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.PreAccept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       cmds,
		Deps:       deps,
		Ballot:     largerBallot,
	})

	// should get broadcastAction and accept message with commands = no-op
	i = commonTestlibExamplePreparingInstance()
	i.initRecoveryInfo()
	i.recoveryInfo.status = nilStatus
	i.recoveryInfo.cmds = nil
	i.recoveryInfo.deps = data.Dependencies{0, 0, 0, 0, 0}
	i.recoveryInfo.identicalCount = i.replica.quorum() - 1

	act, msg = i.makeRecoveryDecision()
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Accept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       nil,
		Deps:       data.Dependencies{0, 0, 0, 0, 0},
		Ballot:     i.ballot,
	})

	i.recoveryInfo.status = nilStatus - 1
	assert.Panics(t, func() { i.makeRecoveryDecision() })
}

func TestHandleTimeout(t *testing.T) {
	// TODO: enterpreparing
	i := commonTestlibExampleNilStatusInstance()
	act, msg := i.handleTimeout(&data.Timeout{})
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Prepare{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     data.NewBallot(1, 1, 0),
	})

	i = commonTestlibExamplePreAcceptedInstance()
	act, msg = i.handleTimeout(&data.Timeout{})
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Prepare{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     data.NewBallot(1, 1, 0),
	})

	i = commonTestlibExampleAcceptedInstance()
	act, msg = i.handleTimeout(&data.Timeout{})
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Prepare{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     data.NewBallot(1, 1, 0),
	})

	i = commonTestlibExamplePreparingInstance()
	act, msg = i.handleTimeout(&data.Timeout{})
	assert.Equal(t, act, broadcastAction)
	assert.Equal(t, msg, &data.Prepare{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     data.NewBallot(1, 2, 0),
	})

	i = commonTestlibExampleCommittedInstance()
	assert.Panics(t, func() { i.handleTimeout(&data.Timeout{}) })
}

// Tests for getters
func TestInstanceGetters(t *testing.T) {
	cmds := commonTestlibExampleCommands()
	deps := commonTestlibExampleDeps()

	i := &Instance{
		cmds: cmds,
		deps: deps,
	}

	assert.Equal(t, i.Commands(), cmds)
	assert.Equal(t, i.Dependencies(), deps)
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

func TestExecuted(t *testing.T) {
	i := &Instance{}
	assert.False(t, i.isExecuted())
	i.SetExecuted()
	assert.True(t, i.isExecuted())
}
