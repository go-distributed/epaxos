package replica

// This file implements replica module.
// @decision (02/01/14):
// - Replica Epoch starts from 1. 0 is reserved for fresly created instance.
// - Now freshly created instance always has the minimum ballot.

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
const defaultInstancesLength = 1024

// actions
const (
	noAction uint8 = iota + 1
	replyAction
	fastQuorumAction
	majoritySendAction
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
		Epoch:          1,
	}

	for i := uint8(0); i < size; i++ {
		r.InstanceMatrix[i] = make([]*Instance, defaultInstancesLength)
		r.MaxInstanceNum[i] = conflictNotFound
	}

	return r
}

func (r *Replica) fastQuorum() int {
	if r.Size < 2 {
		panic("")
	}
	return int(r.Size - 2)
}

func (r *Replica) makeInitialBallot() *data.Ballot {
	return data.NewBallot(r.Epoch, 0, r.Id)
}

func (r *Replica) makeInitialDeps() data.Dependencies {
	return make(data.Dependencies, r.Size)
}

// ***********************
// ***** Seq, Deps *******
// ***********************

// findDependencies finds the most recent interference instance from each instance space
// of this replica.
// It returns (seq, cmds)
// seq = 1 + max{i.seq, where haveconflicts(i.cmds, cmds)} || 0
// cmds = most recent interference instance for each instance space
// TODO: This should be atomic operation in perspective of individual instance.
func (r *Replica) findDependencies(cmds data.Commands) (uint32, data.Dependencies) {
	deps := make(data.Dependencies, r.Size)
	seq := uint32(0)

	for i := range r.InstanceMatrix {
		instances := r.InstanceMatrix[i]
		start := r.MaxInstanceNum[i]

		if conflict, ok := r.scanConflicts(instances, cmds, start, conflictNotFound); ok {
			deps[i] = conflict
			if instances[conflict].seq >= seq {
				seq = instances[conflict].seq + 1
			}
		}
	}
	return seq, deps
}

// updateDependencies updates the passed in dependencies from replica[from].
// return seq, updated dependencies and whether the dependencies has changed.
func (r *Replica) updateDependencies(cmds data.Commands, seq uint32, deps data.Dependencies, from uint8) (uint32, data.Dependencies, bool) {
	changed := false

	for curr := range r.InstanceMatrix {
		// short cut here, the sender knows the latest dependencies for itself
		if curr == int(from) {
			continue
		}

		instances := r.InstanceMatrix[curr]
		start, end := r.MaxInstanceNum[curr], deps[curr]

		if conflict, ok := r.scanConflicts(instances, cmds, start, end); ok {
			changed = true
			deps[curr] = conflict
			if instances[conflict].seq >= seq {
				seq = instances[conflict].seq + 1
			}
		}
	}

	return seq, deps, changed
}

// scanConflicts scans the instances from start to end (high to low).
// return the highest instance that has conflicts with passed in cmds.
func (r *Replica) scanConflicts(instances []*Instance, cmds data.Commands, start uint64, end uint64) (uint64, bool) {
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
