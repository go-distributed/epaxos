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

const defaultInstanceNum = 1024

// actions
const (
	noAction uint8 = iota + 1
	replyAction
	fastQuorumAction
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
	if size%2 == 0 {
		panic("size should be an odd number")
	}
	r = &Replica{
		Id:             replicaId,
		Size:           size,
		MaxInstanceNum: make([]uint64, size),
		InstanceMatrix: make([][]*Instance, size),
		StateMachine:   sm,
		Epoch:          0,
	}

	for i := uint8(0); i < size; i++ {
		r.InstanceMatrix[i] = make([]*Instance, defaultInstanceNum)
		r.MaxInstanceNum[i] = conflictNotFound
	}

	return r
}

func (r *Replica) MakeInitialBallot() *data.Ballot {
	return data.NewBallot(r.Epoch, 0, r.Id)
}

// ***********************
// ***** Seq, Deps *******
// ***********************

// findDependencies finds the most recent interference instance from each instance space
// of this replica.
// It returns the ids of these instances as an array.
func (r *Replica) findDependencies(cmds []data.Command) (uint32, data.Dependencies) {
	deps := make(data.Dependencies, r.Size)
	seq := uint32(0)

	for i := range r.InstanceMatrix {
		instances := r.InstanceMatrix[i]
		start := r.MaxInstanceNum[i]

		if conflict, ok := r.scanConflicts(instances, cmds, start, 0); ok {
			deps[i] = conflict
			if instances[conflict].seq >= seq {
				seq = instances[conflict].seq + 1
			}
		}
	}

	return seq, deps
}

// scanConflicts scans the instances from start to end (high to low).
// return the highest instance that has conflicts with passed in cmds.
func (r *Replica) scanConflicts(instances []*Instance, cmds []data.Command, start uint64, end uint64) (uint64, bool) {
	for i := start; i > end; i-- {
		if instances[i] == nil {
			continue
		}
		// we only need to find the highest instance in conflict
		if r.StateMachine.HaveConflicts(cmds, instances[i].cmds) {
			return i, true
		}
	}

	return conflictNotFound, false
}
