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

func commonTestlibExampleDeps() data.Dependencies {
	return data.Dependencies{
		1, 2, 1, 3, 8,
	}
}

func commonTestlibExampleInstance() *Instance {
	r := New(0, 5, new(test.DummySM))
	i := NewInstance(r, conflictNotFound+1)
	i.cmds = commonTestlibExampleCommands()
	i.deps = commonTestlibExampleDeps(),
	return i
}

func commonTestlibExampleNilStatusInstance() *Instance {
	i := commonTestlibExampleInstance()
	i.status = nilStatus
	return i
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
	copyInstanceInfo = &InstanceInfo{
		isFastPath:     inst.info.isFastPath,
		preAcceptCount: inst.info.preAcceptCount,
		acceptCount:    inst.info.acceptCount,
	}

	var copyReceveryInfo *RecoveryInfo
	copyReceveryInfo = nil
	if inst.recoveryInfo != nil {
		copyReceveryInfo = &RecoveryInfo{
			preAcceptedCount:  inst.recoveryInfo.preAcceptedCount,
			replyCount:        inst.recoveryInfo.replyCount,
			maxAcceptedBallot: inst.recoveryInfo.maxAcceptedBallot.GetCopy(),
			cmds:              inst.recoveryInfo.cmds.GetCopy(),
			deps:              inst.recoveryInfo.deps.GetCopy(),
			status:            inst.recoveryInfo.status,
			formerStatus:      inst.recoveryInfo.formerStatus,
		}
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

// When preAccepted instance receives a pre-accept,
// If ballot > self ballot,
// if preAcceptedProcess accepts or
// rejects the PreAccept message correctly
func TestPreAcceptedProcessPreAccept(t *testing.T) {
	inst := commonTestlibExamplePreAcceptedInstance()
	instanceBallot := data.NewBallot(2, 3, inst.replica.Id)
	smallerBallot := data.NewBallot(2, 2, inst.replica.Id)
	largerBallot := data.NewBallot(2, 4, inst.replica.Id)

	inst.ballot = instanceBallot

	// PreAccept with smaller ballot
	p := &data.PreAccept{
		Ballot: smallerBallot,
	}
	action, reply := inst.preAcceptedProcess(p)

	assert.Equal(t, action, replyAction)
	assert.Equal(t, reply, &data.PreAcceptReply{
		Ok:         false,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Ballot:     instanceBallot,
	})

	expectedSeq := uint32(42)
	expectedDeps := data.Dependencies{1, 0, 0, 8, 6}

	// PreAccept with larger ballot
	p = &data.PreAccept{
		Cmds:   commonTestlibExampleCommands(),
		Deps:   expectedDeps,
		Seq:    expectedSeq,
		Ballot: largerBallot,
	}
	action, reply = inst.preAcceptedProcess(p)

	assert.Equal(t, action, replyAction)
	assert.Equal(t, reply, &data.PreAcceptReply{
		Ok:         true,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Seq:        expectedSeq,
		Deps:       expectedDeps,
		Ballot:     largerBallot,
	})
}

// **********************
// *****  ACCEPTED ******
// **********************

// TestAcceptedProcessWithRejectPreAccept asserts that
// when an accepted instance receives preaccept, it should reject it.
func TestAcceptedProcessWithRejectPreAccept(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	copyInst := commonTestlibGetCopyInstance(inst)
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
	assert.Equal(t, inst, copyInst)
}

// TestAcceptedProcessWithHandleAccept asserts that
// when an accepted instance receives accept, it should handle it if
// the ballot of the message is larger than that of the instance.
func TestAcceptedProcessWithHandleAccept(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	copyInst := commonTestlibGetCopyInstance(inst)
	// create small and large ballots
	smallBallot := inst.replica.makeInitialBallot()
	largeBallot := smallBallot.GetIncNumCopy()

	inst.ballot = largeBallot
	// create an Accept message with small ballot, and send it to the instance
	ac := &data.Accept{
		Ballot: smallBallot,
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
	copyInst.ballot = largeBallot
	assert.Equal(t, inst, copyInst)
}

// TestAcceptedProcessWithRejectAccept asserts that
// when an accepted instance receives accept, it should reject it if
// the ballot of the message is smaller than that of the instance.
//func TestAcceptedProcessWithRejectAccept(t *testing.T) {
//	// create an accepted instance
//	inst := commonTestlibExampleAcceptedInstance()
//	// create small and large ballots
//	smallBallot := inst.replica.makeInitialBallot()
//	largeBallot := smallBallot.GetIncNumCopy()
//
//	inst.ballot = smallBallot
//	// create an Accept message with large ballot, and send it to the instance
//	cmds := commonTestlibExampleCommands()
//	deps := commonTestlibExampleDeps()
//	ac := &data.Accept{
//		Cmds:   cmds,
//		Seq:    42,
//		Deps:   deps,
//		Ballot: largeBallot,
//	}
//	action, m := inst.acceptedProcess(ac)
//
//	// expect:
//	// - action: replyAction
//	// - message: AcceptReply with ok == true, ballot = inst.ballot
//	// - instace:
//	//     cmds = accept.cmds,
//	//     seq = accept.seq,
//	//     deps = accept.deps,
//	//     ballot = accept.ballot
//	assert.Equal(t, action, replyAction)
//	assert.Equal(t, m, &data.AcceptReply{
//		Ok:         true,
//		ReplicaId:  inst.replica.Id,
//		InstanceId: inst.id,
//		Ballot:     inst.ballot,
//	})
//	assert.Equal(t, inst.cmds, cmds)
//	assert.Equal(t, inst.seq, 42)
//	assert.Equal(t, inst.deps, deps)
//	assert.Equal(t, inst.status, accepted)
//	assert.Equal(t, inst.ballot, largeBallot)
//}

// TestAcceptedProcessWithHandleCommit asserts that
// when an accepted instance receives commit, it should handle it.
func TestAcceptedProcessWithHandleCommit(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	copyInst := commonTestlibGetCopyInstance(inst)

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
	assert.Equal(t, inst.cmds, cmds)
	assert.Equal(t, inst.seq, 42)
	assert.Equal(t, inst.deps, deps)
}

// TestAcceptedProcessWithHandlePrepare asserts that
// when an accepted instance receives prepare, it should hande it if
// the ballot of the prepare message is larger than that of the instance.
func TestAcceptedProcessWithHandlePrepare(t *testing.T) {
	// create an accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	// create small and large ballots
	smallBallot := inst.replica.makeInitialBallot()
	largeBallot := smallBallot.GetIncNumCopy()

	inst.ballot = smallBallot

	// create a commit message and send it to the instance
	p := &data.Prepare{
		NeedCmdsInReply: true,
		Ballot: largeBallot,
	}
	action, msg := inst.acceptedProcess(p)

	// expect:
	// - action: replyAction
	// - msg: PrepareReply with 
	
	
	
}

// **********************
// ***** COMMITTED ******
// **********************

// When a committed instance receives:
// * pre-accept reply,
// it should ignore it
func TestCommittedProcessWithNoAction(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	copyInst := commonTestlibGetCopyInstance(inst)
	// send a pre-accept message to it
	pa := &data.PreAcceptReply{}
	action, m := inst.committedProcess(pa)

	// expect:
	// - action: NoAction
	// - message: nil
	// - instance: nothing changed
	assert.Equal(t, action, noAction)
	assert.Nil(t, m)
	assert.Equal(t, inst, copyInst)
}

// If a committed instance receives accept, it will reject it.
func TestCommittedProcessWithRejcetAccept(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	copyInst := commonTestlibGetCopyInstance(inst)
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
	assert.Equal(t, inst, copyInst)
}

// if a committed instance receives prepare with
// - larger ballot, reply ok = true with large ballot
// - smaller ballot, reply ok = true with small ballot.
func TestCommittedProcessWithHandlePrepare(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	copyInst := commonTestlibGetCopyInstance(inst)

	// create small and large ballots
	smallBallot := inst.replica.makeInitialBallot()
	largeBallot := smallBallot.GetIncNumCopy()

	// send a Prepare message to it
	p := &data.Prepare{
		Ballot:          largeBallot,
		NeedCmdsInReply: true,
	}
	expectedReply := &data.PrepareReply{
		Ok:         true,
		ReplicaId:  inst.replica.Id,
		InstanceId: inst.id,
		Status:     committed,
		Cmds:       inst.cmds,
		Deps:       inst.deps,
	}

	// expect:
	// - action: replyAction
	// - message: AcceptReply with ok == true, ballot == message's ballot
	// - instance: nothing changed

	// handle larger ballot
	action, m := inst.committedProcess(p)
	assert.Equal(t, action, replyAction)

	expectedReply.Ballot = largeBallot.GetCopy()
	expectedReply.FromInitialLeader = true
	assert.Equal(t, m, expectedReply)
	assert.Equal(t, inst, copyInst)

	// handle smaller ballot
	inst.ballot = largeBallot
	copyInst.ballot = largeBallot

	p.Ballot = smallBallot
	_, m = inst.committedProcess(p)

	expectedReply.Ballot = smallBallot.GetCopy()
	expectedReply.FromInitialLeader = false
	assert.Equal(t, m, expectedReply)
	assert.Equal(t, inst, copyInst)
}

// committed instance should reject pre-accept
func TestCommittedProcessWithRejectPreAccept(t *testing.T) {
	// create a committed instance
	inst := commonTestlibExampleCommittedInstance()
	copyInst := commonTestlibGetCopyInstance(inst)

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
	assert.Equal(t, inst, copyInst)
}

func TestCommittedProccessWithPanic(t *testing.T) {
	// create a accepted instance
	inst := commonTestlibExampleAcceptedInstance()
	copyInst := commonTestlibGetCopyInstance(inst)
	p := &data.Propose{}
	// expect:
	// - action: will panic if is not at committed status
	// - instance: nothing changed
	assert.Panics(t, func() { inst.committedProcess(p) })
	assert.Equal(t, inst, copyInst)

	// create a committed instance
	inst = commonTestlibExampleCommittedInstance()
	copyInst = commonTestlibGetCopyInstance(inst)

	// expect:
	// - action: will panic if receiving propose
	// - instance: nothing changed
	assert.Panics(t, func() { inst.committedProcess(p) })
	assert.Equal(t, inst, copyInst)
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
	expectedBallot := data.NewBallot(1, 2, inst.replica.Id)
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
	i := commonTestlibExampleCommittedInstance()
	i.ballot = i.replica.makeInitialBallot()
	i.deps = data.Dependencies{3, 4, 5, 6, 7}

	largerBallot := i.ballot.GetIncNumCopy()

	// NeedCmdsInReply == false
	prepare := &data.Prepare{
		ReplicaId:       i.replica.Id,
		InstanceId:      i.id,
		Ballot:          largerBallot,
		NeedCmdsInReply: false,
	}

	action, reply := i.handlePrepare(prepare)

	assert.Equal(t, action, replyAction)
	assert.Equal(t, reply, &data.PrepareReply{
		Ok:                true,
		ReplicaId:         0,
		InstanceId:        1,
		Status:            committed,
		Cmds:              nil,
		Deps:              i.deps.GetCopy(),
		Ballot:            prepare.Ballot,
		FromInitialLeader: true,
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
