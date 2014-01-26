package replica

import (
	"fmt"

	"github.com/go-epaxos/epaxos"
)

var _ = fmt.Printf

// ****************************
// *****  CONST ENUM **********
// ****************************
const (
	conflictNotFound = 0
)

// actions
const (
	noAction int8 = iota + 1
)

// ****************************
// ***** TYPE STRUCT **********
// ****************************

type Replica struct {
	Id             int
	Size           int
	MaxInstanceNum []uint64 // the highest instance number seen for each replica
	InstanceMatrix [][]*Instance
	StateMachine   epaxos.StateMachine
	Epoch          uint32
}

func New(replicaId, size int, sm epaxos.StateMachine) (r *Replica) {
	r = &Replica{
		Id:             replicaId,
		Size:           size,
		MaxInstanceNum: make([]uint64, size),
		InstanceMatrix: make([][]*Instance, size),
		StateMachine:   sm,
		Epoch:          0,
	}

	for i := 0; i < size; i++ {
		r.InstanceMatrix[i] = make([]*Instance, 1024)
		r.MaxInstanceNum[i] = conflictNotFound
	}

	return r
}
