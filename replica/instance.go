package replica

// This file implements instance module.
// @assumption:
// - When a replica pass in the message to instance methods, we assume that the
//    internal fields of message is readable only and safe to reference to.

import (
	"fmt"

	"github.com/go-epaxos/epaxos/data"
)

var _ = fmt.Printf

// ****************************
// *****  CONST ENUM **********
// ****************************

// instance status
const (
	nilStatus uint8 = iota + 1
	preparing
	preAccepted
	accepted
	committed
	executed
)

// ****************************
// ***** TYPE STRUCT **********
// ****************************

type Instance struct {
	cmds   data.Commands
	seq    uint32
	deps   data.Dependencies
	status uint8
	ballot *data.Ballot

	info         *InstanceInfo
	recoveryInfo *RecoveryInfo

	// local information
	replica *Replica
	id      uint64
}

// bookkeeping struct for recording counts of different messages and some flags
type InstanceInfo struct {
	preAcceptCount     int
	preAcceptNackCount int
	isFastPath         bool

	acceptCount     int
	acceptNackCount int

	prepareCount     int
	prepareNackCount int
}

type RecoveryInfo struct {
	preAcceptedCount  int
	replyCount        int
	maxAcceptedBallot *data.Ballot

	cmds         data.Commands
	deps         data.Dependencies
	status       uint8
	formerStatus uint8
}

// ****************************
// **** NEW INSTANCE **********
// ****************************

func NewInstance(replica *Replica, instanceId uint64) (i *Instance) {
	i = &Instance{
		replica: replica,
		id:      instanceId,
	}
	return i
}

func NewInstanceInfo() *InstanceInfo {
	return &InstanceInfo{
		isFastPath: true,
	}
}

func NewRecoveryInfo() *RecoveryInfo {
	return &RecoveryInfo{}
}

// *********************************
// ******** INSTANCE FIELDS  *******
// *********************************
func (i *Instance) isAtStatus(status uint8) bool {
	return i.status == status
}

func (i *Instance) isAfterStatus(status uint8) bool {
	return i.status > status
}

func (i *Instance) isAtOrAfterStatus(status uint8) bool {
	return i.status >= status
}

func (i *Instance) freshlyCreated() bool {
	if i.cmds != nil || i.seq != 0 || i.deps != nil ||
		i.ballot != nil || i.info != nil || i.recoveryInfo != nil {
		return false
	}
	return true
}

// ******************************
// ****** State Processing ******
// ******************************

// TODO: building
func (i *Instance) nilStatusProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(preAccepted)

	if !i.isAtStatus(nilStatus) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.Propose:
		if !i.freshlyCreated() {
			panic("")
		}
		return i.handlePropose(content)
	default:
		panic("")
	}
}

// TODO: finishing building, need test
func (i *Instance) committedProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(committed)

	if !i.isAtStatus(committed) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAccept:
		return i.rejectPreAccept()
	case *data.Accept:
		return i.rejectAccept()
	case *data.Prepare:
		return i.handlePrepare(content)
	case *data.PreAcceptReply, *data.PreAcceptOk, *data.AcceptReply, *data.PrepareReply, *data.Commit:
		// ignore delayed replies
		return noAction, nil
	default:
		panic("")
	}
}

// TODO: building
func (i *Instance) acceptedProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(accepted, committed)

	if !i.isAtStatus(accepted) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAcceptReply, *data.PreAcceptOk, *data.AcceptReply, *data.PrepareReply:
		// ignore delayed replies
		return noAction, nil
	case *data.PreAccept:
		return i.rejectPreAccept()
	case *data.Accept:
		return i.rejectAccept()
	case *data.Commit:
		return i.handleCommit(content)
	case *data.Prepare:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPrepare()
		}
		return i.handlePrepare(content)
	default:
	}
	panic("")
}

// TODO: building
func (i *Instance) preAcceptedProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(preAccepted, accepted, committed)

	if !i.isAtStatus(preAccepted) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAccept:
		_ = content
		panic("")
	case *data.PreAcceptReply, *data.PreAcceptOk, *data.AcceptReply, *data.PrepareReply:
		// ignore delayed replies
		return noAction, nil
	}
	panic("")
}

// ******************************
// ****** Reject Messages *******
// ******************************

// rejectPreAccept rejects the PreAccept request with a PreAcceptReply:
// -Ok: false
// -Ballot: self.ballot
// -ReplicaId: self.replica.id
// -InstanceId: self.id
// -other fields: undefined
func (i *Instance) rejectPreAccept() (action uint8, msg Message) {
	// TODO: assert condition holds
	msg = &data.PreAcceptReply{
		Ok:         false,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Ballot:     i.ballot.GetCopy(),
	}
	action = replyAction
	return
}

// rejectAccept rejects the Accept request with a AcceptReply:
// -Ok: false
// -Ballot: self.ballot
// -ReplicaId: self.replica.id
// -InstanceId: self.id
// -other fields: undefined
func (i *Instance) rejectAccept() (action uint8, msg Message) {
	// TODO: assert contition holds
	msg = &data.AcceptReply{
		Ok:         false,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Ballot:     i.ballot.GetCopy(),
	}
	action = replyAction
	return
}

// rejectPrepare rejects Prepare request with a PrepareReply:
// - Ok: false
// - Ballot: self (ballot)
// - Status: self status
// - relevant Ids
func (i *Instance) rejectPrepare() (action uint8, msg Message) {
	msg = &data.PrepareReply{
		Ok:         false,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Status:     i.status,
		Ballot:     i.ballot.GetCopy(),
	}
	action = replyAction
	return
}

// ******************************
// ****** Handle Message  *******
// ******************************

// when handling propose, a propose will broadcast to fast quorum pre-accept messages.
func (i *Instance) handlePropose(p *data.Propose) (action uint8, msg Message) {
	if p.Cmds == nil {
		panic("")
	}
	seq, deps := i.replica.findDependencies(p.Cmds)
	msg = &data.PreAccept{
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Cmds:       p.Cmds.GetCopy(),
		Seq:        seq,
		Deps:       deps.GetCopy(),
		Ballot:     i.replica.MakeInitialBallot(),
	}

	i.cmds = p.Cmds.GetCopy()
	i.deps = deps.GetCopy()
	i.status = preAccepted
	i.ballot = i.replica.MakeInitialBallot()
	i.info = NewInstanceInfo()

	action = fastQuorumAction
	return
}

func (i *Instance) handlePreAccept(p *data.PreAccept) (action uint8, msg Message) {
	panic("")
}

func (i *Instance) handleAccept(a *data.Accept) (action uint8, msg Message) {
	panic("")
}

// TODO: need testing
func (i *Instance) handleCommit(c *data.Commit) (action uint8, msg Message) {
	if i.isAtOrAfterStatus(committed) {
		panic("")
	}

	i.cmds = c.Cmds
	i.deps = c.Deps
	i.status = committed

	// TODO: Do we need to clear unnecessary objects to save more memory?
	// TODO: persistent
	action = noAction
	msg = nil
	return
}

// handlePrepare handles Prepare messages
func (i *Instance) handlePrepare(p *data.Prepare) (action uint8, msg Message) {
	i.ballot = p.Ballot

	cmds := data.Commands(nil)
	if p.NeedCmdsInReply {
		cmds = i.cmds.GetCopy()
	}

	msg = &data.PrepareReply{
		Ok:         true,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Status:     i.status,
		Cmds:       cmds,
		Deps:       i.deps.GetCopy(),
		Ballot:     i.ballot.GetCopy(),
	}
	action = replyAction
	return action, msg
}

// checkStatus checks the status of the instance
// it panics if the instance's status is not as expected
func (i *Instance) checkStatus(statusList ...uint8) {
	ok := false
	for _, status := range statusList {
		if i.isAtStatus(status) {
			ok = true
			break
		}
	}
	if !ok {
		panic("")
	}
}

// ****************************
// ******* Make Message *******
// ****************************
