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
	cmds   []data.Command
	seq    uint32
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

	cmds   []data.Command
	deps   dependencies
	status int8
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

// ******************************
// ****** State Processing ******
// ******************************

func (i *Instance) committedProcess(m Message) (int8, Message) {

	return noAction, nil
}
