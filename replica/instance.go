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
	nilStatus int8 = iota + 1
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
	cmds data.Commands
	seq  uint32
	//deps   []uint64
	deps   data.Dependencies
	status int8
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

	acceptNackCount int
	acceptCount     int

	prepareCount int
}

type RecoveryInfo struct {
	preAcceptedCount  int
	replyCount        int
	maxAcceptedBallot *data.Ballot

	cmds         data.Commands
	deps         data.Dependencies
	status       int8
	formerStatus int8
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

// ****************************
// ******** tell status *******
// ****************************
func (i *Instance) isAtStatus(status int8) bool {
	return i.status == status
}

func (i *Instance) isAfterStatus(status int8) bool {
	return i.status > status
}

func (i *Instance) isAtOrAfterStatus(status int8) bool {
	return i.status >= status
}

// ******************************
// ****** State Processing ******
// ******************************

func (i *Instance) committedProcess(m Message) (int8, Message) {
	return noAction, nil
}

// acceptProcess will handle:
// Commit, Preparing
// will ignore:
// PreAccept, PreAcceptReply, Accept, AcceptReply, PrepareReply
func (i *Instance) acceptedProcess(m Message) (int8, Message) {
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
		return i.handlePrepare(content)
	default:
		panic("")
	}
}

func (i *Instance) handlePreAccept(p *data.PreAccept) (int8, Message) {
	panic("")
}

func (i *Instance) handleAccept(a *data.Accept) (int8, Message) {
	panic("")
}

func (i *Instance) handleCommit(c *data.Commit) (int8, Message) {
	if i.isAtOrAfterStatus(committed) {
		panic("")
	}

	i.cmds = c.Cmds
	i.deps = c.Deps
	i.status = committed
	// TODO: persistent
	return noAction, nil
}

func (i *Instance) handlePrepare(p *data.Prepare) (int8, Message) {
	// TODO: can delete this assertion
	if i.isAtStatus(nilStatus) {
		if i.ballot.Compare(data.MakeInitialBallot(i.replica.Id)) != 0 {
			panic("ballot should be initial ballot")
		}
	}

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
