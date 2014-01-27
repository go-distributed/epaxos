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

func (i *Instance) nilStatusProcess(m Message) (uint8, Message) {
	if i.status != nilStatus {
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

func (i *Instance) committedProcess(m Message) (uint8, Message) {
	if i.status != committed {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAcceptReply:
		content = content
		return noAction, nil
	default:
		panic("")
	}
}

func (i *Instance) acceptedProcess(m Message) (uint8, Message) {
	if i.status != accepted {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAcceptReply, *data.PreAcceptOk, *data.AcceptReply, *data.PrepareReply:
		return noAction, nil
	case *data.PreAccept:
		return i.handlePreAccept(content)
	case *data.Accept:
		return i.handleAccept(content)
	case *data.Commit:
		return i.handleCommit(content)
	case *data.Prepare:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPrepare()
		}
		return i.handlePrepare(content)
	default:
		panic("")
	}
}

// ******************************
// ****** Reject Message  *******
// ******************************

// Prepare reply:
// - ok : false
// - Ballot: self (ballot)
// - Status: self status
// - relevant Ids
func (i *Instance) rejectPrepare() (uint8, Message) {
	pr := &data.PrepareReply{
		Ok:         false,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Status:     i.status,
		Ballot:     i.ballot.GetCopy(),
	}

	return replyAction, pr
}

// ******************************
// ****** Handle Message  *******
// ******************************

// when handling propose, a propose will broadcast to fast quorum pre-accept messages.
func (i *Instance) handlePropose(p *data.Propose) (uint8, Message) {
	if p.Cmds == nil {
		panic("")
	}
	seq, deps := i.replica.findDependencies(p.Cmds)
	pa := &data.PreAccept{
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

	return fastQuorumAction, pa
}

func (i *Instance) handlePreAccept(p *data.PreAccept) (uint8, Message) {
	panic("")
}

func (i *Instance) handleAccept(a *data.Accept) (uint8, Message) {
	panic("")
}

func (i *Instance) handleCommit(c *data.Commit) (uint8, Message) {
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

func (i *Instance) handlePrepare(p *data.Prepare) (uint8, Message) {
	i.ballot = p.Ballot

	cmds := data.Commands(nil)
	if p.NeedCmdsInReply {
		cmds = i.cmds.GetCopy()
	}

	pr := &data.PrepareReply{
		Ok:         true,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Status:     i.status,
		Cmds:       cmds,
		Deps:       i.deps.GetCopy(),
		Ballot:     i.ballot.GetCopy(),
	}
	return replyAction, pr
}

// ****************************
// ******* Make Message *******
// ****************************
