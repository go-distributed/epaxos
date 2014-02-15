package replica

// This file implements replica module.
// @decision (02/01/14):
// - Replica Epoch starts from 1. 0 is reserved for fresly created instance.
// - Now freshly created instance always has the minimum ballot.

import (
	"fmt"

	"github.com/go-distributed/epaxos"
	"github.com/go-distributed/epaxos/data"
)

var _ = fmt.Printf

// ****************************
// *****  CONST ENUM **********
// ****************************
const defaultInstancesLength = 1024*1024
const conflictNotFound = 0
const epochStart = 1

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
	EventChan      chan *Event
	Transporter
}

type Event struct {
	From    uint8
	Message Message
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
		Epoch:          epochStart,
		// TODO: decide channel buffer length
		EventChan: make(chan *Event),
	}

	for i := uint8(0); i < size; i++ {
		r.InstanceMatrix[i] = make([]*Instance, defaultInstancesLength)
		r.MaxInstanceNum[i] = conflictNotFound
	}

	return r
}

// Start running the replica. It shouldn't stop at any time.
func (r *Replica) Start() {
	for {
		// TODO: check timeout
		// add time.After for timeout checking
		select {
		case event := <-r.EventChan:
			r.dispatch(event)
		}
	}
}
func (r *Replica) GoStart() { go r.Start() }

// TODO: This must be done in an synchronized/atomic way.
func (r *Replica) Propose(cmds data.Commands) {
	event := &Event{
		From: r.Id,
		Message: &data.Propose{
			ReplicaId:  r.Id,
			InstanceId: r.MaxInstanceNum[r.Id] + 1,
			Cmds:       cmds,
		},
	}
	r.dispatch(event)
}

// *****************************
// ***** Message Handling ******
// *****************************

// This function is responsible for communicating with instance processing.
func (r *Replica) dispatch(event *Event) {
	eventMsg := event.Message
	replicaId := eventMsg.Replica()
	instanceId := eventMsg.Instance()

	if instanceId <= conflictNotFound {
		panic("")
	}

	if r.InstanceMatrix[replicaId][instanceId] == nil {
		r.InstanceMatrix[replicaId][instanceId] = NewInstance(r, replicaId, instanceId)
	}

	i := r.InstanceMatrix[replicaId][instanceId]
	var action uint8
	var msg Message

	switch i.status {
	case nilStatus:
		action, msg = i.nilStatusProcess(eventMsg)
	case preAccepted:
		action, msg = i.preAcceptedProcess(eventMsg)
	case accepted:
		action, msg = i.acceptedProcess(eventMsg)
	case committed:
		action, msg = i.committedProcess(eventMsg)
	default:
		panic("")
	}

	switch action {
	case noAction:
		return
	case replyAction:
		r.Transporter.Send(event.From, msg)
	case fastQuorumAction:
		r.Transporter.MulticastFastquorum(msg)
	case broadcastAction:
		r.Transporter.Broadcast(msg)
	default:
		panic("")

	}
}

// **************************
// Instance Related Fields
// **************************

func (r *Replica) fastQuorum() int {
	if r.Size < 2 {
		panic("")
	}
	return int(r.Size - 2)
}

func (r *Replica) quorum() int {
	return int(r.Size / 2)
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

// This func finds the most recent interference instance from each instance space
// of this replica.
// It returns (seq, cmds)
// seq = 1 + max{i.seq, where haveconflicts(i.cmds, cmds)} || 0
// cmds = most recent interference instance for each instance space
// TODO: This should be atomic operation in perspective of individual instance.
func (r *Replica) initInstance(cmds data.Commands, i *Instance) {
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
	i.cmds, i.seq, i.deps = cmds, seq, deps
	// we can only update here because
	// now we are safe to have cmds, etc. inside instance
	i.replica.updateMaxInstanceNum(i.rowId, i.id)
}

// This func updates the passed in dependencies from replica[from].
// return seq, updated dependencies and whether the dependencies has changed.
func (r *Replica) updateInstance(cmds data.Commands, seq uint32, deps data.Dependencies, from uint8, i *Instance) bool {
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

	i.cmds, i.seq, i.deps = cmds, seq, deps
	// we can only update here because
	// now we are safe to have cmds, etc. inside instance
	i.replica.updateMaxInstanceNum(i.rowId, i.id)
	return changed
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

func (r *Replica) updateMaxInstanceNum(rowId uint8, instanceId uint64) bool {
	if r.MaxInstanceNum[rowId] < instanceId {
		r.MaxInstanceNum[rowId] = instanceId
		return true
	}
	return false
}
