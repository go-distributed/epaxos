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
	cmds []data.Command
	seq  uint32
	//deps   []uint64
	deps   dependencies
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

	cmds         []data.Command
	deps         dependencies
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

// ***************************************
// ******** get copies of some fields ****
// ***************************************

func (i *Instance) getCmdsCopy() []data.Command {
	cmds := make([]data.Command, len(i.cmds))
	copy(cmds, i.cmds)
	return cmds
}

func (i *Instance) getDepsCopy() dependencies {
	deps := make(dependencies, len(i.deps))
	copy(deps, i.deps)
	return deps
}

func (i *Instance) getBallotCopy() *data.Ballot {
	return i.ballot.GetCopy()
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

func (i *Instance) isEqualOrAfterStatus(status int8) bool {
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
// PreAccept, Accept
func (i *Instance) acceptedProcess(m Message) (int8, Message) {
	switch content := m.Content().(type) {
	case *data.PreAccept, *data.PreAcceptReply, *data.PreAcceptOk, *data.Accept, *data.AcceptReply, *data.PrepareReply:
		return noAction, nil
	case *data.Commit:
		return i.handleCommit(content)
	case *data.Prepare:
		return i.handlePrepare(content)
	default:
		panic("")
	}
}

func (i *Instance) handleCommit(c *data.Commit) (int8, Message) {
	if i.isEqualOrAfterStatus(committed) {
		panic("")
	}

	copy(i.cmds, c.Cmds)
	copy(i.deps, c.Deps)
	i.status = committed
	// TODO: persistent
	return noAction, nil
}

func (i *Instance) handlePrepare(p *data.Prepare) (int8, Message) {
	ok := false
	if p.Ballot.Compare(i.ballot) > 0 {
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
		Cmds:       i.getCmdsCopy(),
		Deps:       i.getDepsCopy(),
		Ballot:     i.getBallotCopy(),
	}

	return reply, pr
}
