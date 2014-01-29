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
	preAcceptCount int
	isFastPath     bool

	acceptCount int

	prepareCount int
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
	if i.ballot == nil {
		return true
	}
	return false
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
	case *data.PreAccept:
		if !i.freshlyCreated() && content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPreAccept()
		}
		return i.handlePreAccept(content)
	default:
		panic("")
	}
}

// TODO: finish building, need testing
func (i *Instance) preAcceptedProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(preAccepted, accepted, committed)

	if !i.isAtStatus(preAccepted) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAccept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPreAccept()
		}
		return i.handlePreAccept(content)

	case *data.Accept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectAccept()
		}
		return i.handleAccept(content)

	case *data.Commit:
		return i.handleCommit(content)
	case *data.Prepare:
		return i.handlePrepare(content)
	case *data.PreAcceptReply:
		if content.Ballot.Compare(i.ballot) < 0 {
			// ignore stale PreAcceptReply
			return noAction, nil
		}
		return i.handlePreAcceptReply(content)

	case *data.PreAcceptOk, *data.AcceptReply, *data.PrepareReply:
		// ignore delayed replies
		return noAction, nil
	default:
		panic("")
	}
}

// finish building, need testing
func (i *Instance) acceptedProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(accepted, committed)

	if !i.isAtStatus(accepted) {
		panic("")
	}

	switch content := m.Content().(type) {
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

	case *data.AcceptReply:
		if content.Ballot.Compare(i.ballot) < 0 {
			// ignore stale PreAcceptReply
			return noAction, nil
		}
		return i.handleAcceptReply(content)

	case *data.PreAcceptReply, *data.PreAcceptOk, *data.PrepareReply:
		// ignore delayed replies
		return noAction, nil
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
func (i *Instance) prepareProcess(m Message) (action uint8, msg Message) {
	panic("")
}

// ******************************
// ****** Reject Messages *******
// ******************************

// PreAccept reply:
// - ok : false
// - Ballot: self (ballot)
// - Ids
func (i *Instance) rejectPreAccept() (action uint8, reply *data.PreAcceptReply) {
	return replyAction, i.makePreAcceptReply(false, 0, nil)
}

// rejectAccept rejects the Accept request with a AcceptReply:
// - Ok: false
// - Ballot: self.ballot
// - ReplicaId: self.replica.id
// - InstanceId: self.id
// - other fields: undefined
func (i *Instance) rejectAccept() (action uint8, reply *data.AcceptReply) {
	// TODO: assert contition holds
	return replyAction, &data.AcceptReply{
		Ok:         false,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Ballot:     i.ballot.GetCopy(),
	}
}

// Prepare reply:
// - ok : false
// - Ballot: self (ballot)
// - relevant Ids
func (i *Instance) rejectPrepare() (action uint8, reply *data.PrepareReply) {
	return replyAction, &data.PrepareReply{
		Ok:         false,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Ballot:     i.ballot.GetCopy(),
		Status:     i.status, //unnecessary, but it doesn't hurt
	}
}

// ******************************
// ****** Handle Message  *******
// ******************************

// a propose will broadcasted to fast quorum in pre-accept message.
func (i *Instance) handlePropose(p *data.Propose) (action uint8, msg *data.PreAccept) {
	if p.Cmds == nil {
		panic("")
	}
	seq, deps := i.replica.findDependencies(p.Cmds)

	i.cmds = p.Cmds.GetCopy()
	i.seq = seq
	i.deps = deps.GetCopy()
	i.status = preAccepted
	i.ballot = i.replica.MakeInitialBallot()
	i.info = NewInstanceInfo()

	return fastQuorumAction, &data.PreAccept{
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Cmds:       p.Cmds.GetCopy(),
		Seq:        seq,
		Deps:       deps.GetCopy(),
		Ballot:     i.replica.MakeInitialBallot(),
	}
}

// When handling pre-accept, instance will set its ballot to newer one, and
// update seq, deps if find any change.
// Reply: pre-accept-OK if no change in deps; otherwise a normal pre-accept-reply.
// The pre-accept-OK contains just one field. So we do it for network optimization
func (i *Instance) handlePreAccept(p *data.PreAccept) (action uint8, msg Message) {
	i.ballot = p.Ballot
	seq, deps, changed := i.replica.updateDependencies(p.Cmds, p.Seq, p.Deps, p.ReplicaId)

	if changed {
		i.seq, i.deps = seq, deps
		return replyAction, i.makePreAcceptReply(true, seq, deps)
	}
	// not initial leader
	if p.Ballot.GetNumber() != 0 {
		return replyAction, i.makePreAcceptReply(true, seq, deps)
	}
	// pre-accept-ok for possible fast quorum commit
	return replyAction, &data.PreAcceptOk{
		InstanceId: i.id,
	}
}

// handlePreAcceptReply() handles PreAcceptReplies,
// Update:
// - ballot
// - if ok=true, union seq, deps, and update counts
// Broadcast Action happens:
// 1, if receiving >= fast quorum replies with same deps and seq,
//    and the instance itself is initial leader,
//    then broadcast Commit
// 2, otherwise broadcast Accept
func (i *Instance) handlePreAcceptReply(p *data.PreAcceptReply) (action uint8, msg Message) {
	panic("")
}

// handleAccept handles Accept messages,
// Update:
// - cmds, seq, ballot
// action: reply an AcceptReply message, with:
// - Ok = true
// - everything else = instance's fields
func (i *Instance) handleAccept(a *data.Accept) (action uint8, msg *data.AcceptReply) {
	i.cmds = a.Cmds
	i.seq = a.Seq
	i.ballot = a.Ballot

	msg = &data.AcceptReply{
		Ok:         true,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Ballot:     i.ballot.GetCopy(),
	}
	action = replyAction
	return
}

// handleAcceptReply() handles AcceptReplies,
// Update:
// - ballot
// 1, if receiving majority replies with ok == true,
// then broadcast Commit
// 2, otherwise: do nothing.
func (i *Instance) handleAcceptReply(p *data.AcceptReply) (action uint8, msg Message) {
	action = noAction
	msg = nil

	i.ballot = p.Ballot
	if p.Ok {
		i.info.acceptCount++
	}

	if i.info.acceptCount >= int(i.replica.Size/2) {
		action = replyAction
		msg = &data.Commit{
			Cmds:       i.cmds.GetCopy(),
			Seq:        i.seq,
			Deps:       i.deps.GetCopy(),
			ReplicaId:  i.replica.Id,
			InstanceId: i.id,
		}
	}
	return action, msg
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
	return noAction, nil
}

// handlePrepare handles Prepare messages
func (i *Instance) handlePrepare(p *data.Prepare) (action uint8, msg *data.PrepareReply) {
	var cmds data.Commands
	cmds = nil
	if p.NeedCmdsInReply {
		cmds = i.cmds.GetCopy()
	}

	i.ballot = p.Ballot

	return replyAction, &data.PrepareReply{
		Ok:         true,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Status:     i.status,
		Cmds:       cmds,
		Deps:       i.deps.GetCopy(),
		Ballot:     i.ballot.GetCopy(),
	}
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

func (i *Instance) makePreAcceptReply(ok bool, seq uint32, deps data.Dependencies) *data.PreAcceptReply {
	return &data.PreAcceptReply{
		Ok:         ok,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Seq:        seq,
		Deps:       deps,
		Ballot:     i.ballot.GetCopy(),
	}
}
