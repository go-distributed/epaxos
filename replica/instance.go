package replica

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

// ****************************
// ******** tell status *******
// ****************************
func (i *Instance) isAtStatus(status uint8) bool {
	return i.status == status
}

func (i *Instance) isAfterStatus(status uint8) bool {
	return i.status > status
}

func (i *Instance) isAtOrAfterStatus(status uint8) bool {
	return i.status >= status
}

// ******************************
// ****** State Processing ******
// ******************************

func (i *Instance) nilStatusProcess(m Message) (uint8, Message) {
	if !i.isAtStatus(nilStatus) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.Propose:
		if i.cmds != nil || i.seq != 0 || i.deps != nil ||
			i.ballot != nil || i.info != nil || i.recoveryInfo != nil {
			panic("")
		}
		return i.handlePropose(content)
	default:
		panic("")
	}
}

func (i *Instance) committedProcess(m Message) (uint8, Message) {
	defer i.checkStatus(committed)

	if !i.isAtStatus(committed) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAcceptReply, *data.PreAcceptOk, *data.AcceptReply, *data.PrepareReply, *data.Commit:
		// ignore delayed replies
		return noAction, nil
	case *data.Accept:
		//return i.rejectPreAccept(content)
	case *data.Prepare:
		return i.handlePrepare(content)
	default:
		panic("")
	}
	panic("")
}

func (i *Instance) acceptedProcess(m Message) (uint8, Message) {
	defer i.checkStatus(accepted, committed)

	if !i.isAtStatus(accepted) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAcceptReply, *data.PreAcceptOk, *data.AcceptReply, *data.PrepareReply:
		// ignore delayed replies
		return noAction, nil
	case *data.PreAccept:
		//return i.rejectPreAccept(content)
	case *data.Accept:
		//return i.rejectAccept(content)
	case *data.Commit:
		return i.handleCommit(content)
	case *data.Prepare:
		if content.Ballot.Compare(i.ballot) < 0 {
			// return replyAction, negative_reply
		}
		return i.handlePrepare(content)
	default:
		panic("")
	}
	panic("")
}

func (i *Instance) preAcceptedProcess(m Message) (uint8, Message) {
	defer i.checkStatus(preAccepted, accepted, committed)

	if !i.isAtStatus(preAccepted) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAcceptReply, *data.PreAcceptOk, *data.AcceptReply, *data.PrepareReply:
		// ignore delayed replies
		return noAction, nil
	case *data.PreAccept:
		_ = content
		panic("")
	}
	panic("")
}

// ******************************
// ****** Handle Message  *******
// ******************************

// when handling propose, a propose will broadcast to fast quorum pre-accept messages.
func (i *Instance) handlePropose(p *data.Propose) (uint8, Message) {
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
	// TODO: persistent
	return noAction, nil
}

func (i *Instance) handlePrepare(p *data.Prepare) (uint8, Message) {
	ok := false
	if p.Ballot.Compare(i.ballot) > 0 {
		i.ballot = p.Ballot.GetCopy()
		ok = true
	}

	status := i.status
	if status == preparing {
		status = i.recoveryInfo.formerStatus
	}

	pr := &data.PrepareReply{
		Ok:         ok,
		Status:     status,
		ReplicaId:  i.replica.Id,
		InstanceId: i.id,
		Cmds:       i.cmds.GetCopy(),
		Deps:       i.deps.GetCopy(),
		Ballot:     i.ballot.GetCopy(),
	}

	return replyAction, pr
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
