package replica

import (
	"fmt"

	"github.com/go-epaxos/epaxos"
	"github.com/go-epaxos/epaxos/data"
)

var _ = fmt.Printf

// ****************************
// *****  CONST ENUM **********
// ****************************
const conflictNotFound = 0

// actions
const (
	noAction int8 = iota + 1
	replyAction
	broadcastAction
)

// ****************************
// ***** TYPE STRUCT **********
// ****************************

type Replica struct {
	Id             uint8
	Size           uint8
	MaxInstanceNum []uint64 // the highest instance number seen for each replica
	InstanceMatrix [][]*Instance
	StateMachine   epaxos.StateMachine
	Epoch          uint32
}

func New(replicaId, size uint8, sm epaxos.StateMachine) (r *Replica) {
	r = &Replica{
		Id:             replicaId,
		Size:           size,
		MaxInstanceNum: make([]uint64, size),
		InstanceMatrix: make([][]*Instance, size),
		StateMachine:   sm,
		Epoch:          0,
	}

	for i := uint8(0); i < size; i++ {
		r.InstanceMatrix[i] = make([]*Instance, 1024)
		r.MaxInstanceNum[i] = conflictNotFound
	}

	return r
}

func (r *Replica) MakeInitialBallot() *data.Ballot {
	return data.NewBallot(r.Epoch, 0, r.Id)
}
