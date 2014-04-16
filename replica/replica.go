package replica

// This file implements replica module.
// @decision (02/01/14):
// - Replica Epoch starts from 1. 0 is reserved for fresly created instance.
// - Now freshly created instance always has the minimum ballot.
// @decision(02/15/14):
// - An instance will always set dependency on its immediate precessor in the same
// - instance space. This enforces sequential execution in single instance space.
// @decision(02/17/14):
// - Add checkpoint cycle. Any instance conflicts with a checkpoint and vice versa.
// - This is used to decrease the size of conflict scanning space.

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/golang/glog"

	"github.com/go-distributed/epaxos"
	"github.com/go-distributed/epaxos/data"
)

var v1Log = glog.V(0)

var (
	errConflictsNotFullyResolved = errors.New("Conflicts not fully resolved")
)

// ****************************
// *****  CONST ENUM **********
// ****************************
const (
	defaultInstancesLength = 1024 * 64
	conflictNotFound       = 0
	epochStart             = 1
	executeInterval        = 5 * time.Millisecond
)

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
	Id               uint8
	Size             uint8
	MaxInstanceNum   []uint64 // the highest instance number seen for each replica
	ProposeNum       uint64
	ProposeChan      chan data.Command
	BatchInterval    time.Duration
	CheckpointCycle  uint64
	ExecutedUpTo     []uint64
	InstanceMatrix   [][]*Instance
	StateMachine     epaxos.StateMachine
	Epoch            uint32
	MessageEventChan chan *MessageEvent
	Addrs            []string
	Transporter

	// tarjan SCC
	sccStack   *list.List
	sccResults [][]*Instance
	sccIndex   int
}

type Param struct {
	Addrs        []string
	ReplicaId    uint8
	Size         uint8
	StateMachine epaxos.StateMachine
	//CheckpointCycle uint64
	//BatchInterval   time.Duration
}

type MessageEvent struct {
	From    uint8
	Message Message
}

func New(param *Param) (*Replica, error) {
	replicaId := param.ReplicaId
	size := param.Size
	sm := param.StateMachine
	cycle := uint64(1024)

	if size%2 == 0 {
		return nil, fmt.Errorf("Use odd number as quorum size")
	}

	r := &Replica{
		Id:               replicaId,
		Size:             size,
		MaxInstanceNum:   make([]uint64, size),
		ProposeNum:       1, // instance.id start from 1
		ProposeChan:      make(chan data.Command),
		BatchInterval:    5 * time.Millisecond,
		CheckpointCycle:  cycle,
		ExecutedUpTo:     make([]uint64, size),
		InstanceMatrix:   make([][]*Instance, size),
		StateMachine:     sm,
		Epoch:            epochStart,
		MessageEventChan: make(chan *MessageEvent),
		sccStack:         list.New(),
		Addrs:            param.Addrs,
	}

	for i := uint8(0); i < size; i++ {
		r.InstanceMatrix[i] = make([]*Instance, defaultInstancesLength)
		r.MaxInstanceNum[i] = conflictNotFound
	}

	var err error
	r.Transporter, err = NewNetworkTransporter(param.Addrs, r.Id, r.Size)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Start running the replica. It shouldn't stop at any time.
func (r *Replica) Start() error {
	listener, err := net.Listen("udp", r.Addrs[r.Id])
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				glog.Errorln("listener is closed:", err)
				return
			}

			go func(c net.Conn) {
				// TODO: message size re-thought
				data := make([]byte, 8192)
				n, err := c.Read(data)
				if err != nil {
					glog.Errorln("UDP read:", err)
					return
				}

				msgEvent := new(MessageEvent)
				json.Unmarshal(data[:n], msgEvent)
				r.MessageEventChan <- msgEvent
			}(conn)
		}
	}()

	go r.eventLoop()
	//go r.executeLoop()
	go r.proposeLoop()
	return nil
}

// handling events
func (r *Replica) eventLoop() {
	for {
		// TODO: check timeout
		// add time.After for timeout checking
		select {
		case mevent := <-r.MessageEventChan:
			r.dispatch(mevent)
		}
	}
}

func (r *Replica) executeLoop() {
	for {
		time.Sleep(executeInterval)
		// execution of committed instances
		r.findAndExecute()
	}
}

func (r *Replica) proposeLoop() {
	proposeTicker := time.Tick(r.BatchInterval)

	bufferedCmds := make([]data.Command, 0)

	for {
		select {
		case cmd := <-r.ProposeChan:
			bufferedCmds = append(bufferedCmds, cmd)
		case <-proposeTicker:
			if len(bufferedCmds) == 0 {
				break
			}
			r.BatchPropose(bufferedCmds)
			bufferedCmds = bufferedCmds[:0]
		}
	}
}

// TODO: This must be done in a synchronized/atomic way.
func (r *Replica) Propose(cmd data.Command) {
	r.ProposeChan <- cmd
}

// TODO: This must be done in a synchronized/atomic way.
func (r *Replica) BatchPropose(batchedCmds data.Commands) {
	cmds := make([]data.Command, len(batchedCmds))
	copy(cmds, batchedCmds)

	mevent := &MessageEvent{
		From: r.Id,
		Message: &data.Propose{
			ReplicaId:  r.Id,
			InstanceId: r.ProposeNum,
			Cmds:       cmds,
		},
	}
	r.ProposeNum++
	if r.IsCheckpoint(r.ProposeNum) {
		r.ProposeNum++
	}
	r.MessageEventChan <- mevent
}

// *****************************
// ***** Message Handling ******
// *****************************

// This function is responsible for communicating with instance processing.
func (r *Replica) dispatch(mevent *MessageEvent) {
	eventMsg := mevent.Message
	replicaId := eventMsg.Replica()
	instanceId := eventMsg.Instance()

	v1Log.Infof("Replica[%v]: recv message[%s], from Replica[%v]\n",
		r.Id, eventMsg.String(), mevent.From)

	if instanceId <= conflictNotFound {
		panic("")
	}

	if r.InstanceMatrix[replicaId][instanceId] == nil {
		r.InstanceMatrix[replicaId][instanceId] = NewInstance(r, replicaId, instanceId)
	}

	i := r.InstanceMatrix[replicaId][instanceId]
	var action uint8
	var msg Message

	v1Log.Infof("Replica[%v]: instance[%v][%v] status before = %v\n",
		r.Id, replicaId, instanceId, i.StatusString())

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

	if i.isAtStatus(committed) && action == noAction {
		v1Log.Infof("Replica[%v]: instance[%v][%v] status after = %v\n\n",
			r.Id, replicaId, instanceId, i.StatusString())
	} else {
		v1Log.Infof("Replica[%v]: instance[%v][%v] status after = %v\n",
			r.Id, replicaId, instanceId, i.StatusString())
	}

	switch action {
	case noAction:
		return
	case replyAction:
		v1Log.Infof("Replica[%v]: send message[%s], to Replica[%v]\n\n",
			r.Id, msg.String(), mevent.From)
		r.Transporter.Send(mevent.From, msg)
	case fastQuorumAction:
		v1Log.Infof("Replica[%v]: send message[%s], to FastQuorum\n\n",
			r.Id, msg.String(), mevent.From)
		r.Transporter.MulticastFastquorum(msg)
	case broadcastAction:
		v1Log.Infof("Replica[%v]: send message[%s], to Everyone\n\n",
			r.Id, msg.String(), mevent.From)
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

// This func initiate a new instance, construct its commands and dependencies
// TODO: this operation should synchronized/atomic
func (r *Replica) initInstance(cmds data.Commands, i *Instance) {
	if i.rowId != r.Id {
		panic("")
	}

	deps := make(data.Dependencies, r.Size)

	for curr := range r.InstanceMatrix {
		instances := r.InstanceMatrix[curr]
		start := r.MaxInstanceNum[curr]

		if curr == int(i.rowId) {
			// set deps on its immediate precessor
			deps[curr] = uint64(i.id - 1)
			continue
		}

		conflict := r.scanConflicts(instances, cmds, start, 0)
		deps[curr] = conflict
	}
	i.cmds, i.deps = cmds, deps
	// we can only update here because
	// now we are safe to have cmds, etc. inside instance
	if !i.replica.updateMaxInstanceNum(i.rowId, i.id) {
		panic("")
	}
}

// This func updates the passed in dependencies from replica[from].
// return updated dependencies and whether the dependencies has changed.
// TODO: this operation should synchronized/atomic
func (r *Replica) updateInstance(cmds data.Commands, deps data.Dependencies, from uint8, i *Instance) bool {
	changed := false

	for curr := range r.InstanceMatrix {
		// the sender knows the latest dependencies for its instance space
		if curr == int(from) {
			continue
		}

		instances := r.InstanceMatrix[curr]
		start, end := r.MaxInstanceNum[curr], deps[curr]

		conflict := r.scanConflicts(instances, cmds, start, end)
		if deps[curr] < conflict {
			changed = true
			deps[curr] = conflict
		}
	}

	i.cmds, i.deps = cmds, deps
	// we can only update here because
	// now we are safe to have cmds, etc. inside instance
	i.replica.updateMaxInstanceNum(i.rowId, i.id)
	return changed
}

func (r *Replica) IsCheckpoint(n uint64) bool {
	return n%r.CheckpointCycle == 0
}

// scanConflicts scans the instances from start to end (high to low).
// return the highest instance that has conflicts with passed in cmds.
func (r *Replica) scanConflicts(instances []*Instance, cmds data.Commands, start uint64, end uint64) uint64 {
	for i := start; i > end; i-- {
		if r.IsCheckpoint(i) {
			return i
		}
		if instances[i] == nil {
			continue
		}
		// we only need to find the highest instance in conflict
		if r.StateMachine.HaveConflicts(cmds, instances[i].cmds) {
			return i
		}
	}

	return end
}

func (r *Replica) updateMaxInstanceNum(rowId uint8, instanceId uint64) bool {
	if r.MaxInstanceNum[rowId] < instanceId {
		r.MaxInstanceNum[rowId] = instanceId
		return true
	}
	return false
}

// ******************************
// ********  EXECUTION **********
// ******************************

func (r *Replica) findAndExecute() {
	for i := 0; i < int(r.Size); i++ {
		// search this instance space
		for {
			up := r.ExecutedUpTo[i] + 1

			if r.IsCheckpoint(up) {
				r.ExecutedUpTo[i]++
				continue
			}

			instance := r.InstanceMatrix[i][up]
			if instance == nil || !instance.isAtStatus(committed) {
				break
			}
			if instance.isExecuted() {
				r.ExecutedUpTo[i]++
				continue
			}
			if err := r.execute(instance); err != nil {
				switch err {
				case errConflictsNotFullyResolved:
					break
				case epaxos.ErrStateMachineExecution:
					panic("")
					// TODO: log and warning
					break
				default:
					panic("unexpected error")

				}
			}
		}
	}
}

// NOTE: atomic
func (r *Replica) execute(i *Instance) error {
	r.sccStack.Init()
	r.sccResults = make([][]*Instance, 0)
	r.sccIndex = 1
	if ok := r.resolveConflicts(i); !ok {
		return errConflictsNotFullyResolved
	}
	// execute elements in the result list
	// nodes of the list are in order that:
	// - nodes SCC being dependent are at smaller index than
	// - - nodes depending on it.
	// - In the same component, nodes at higher rowId are at smaller index.
	if err := r.executeList(); err != nil {
		return err
	}
	return nil
}

// this should be a transaction.
func (r *Replica) executeList() error {
	cmdsBuffer := make([]data.Command, 0)

	// batch all commands in the scc
	for _, sccNodes := range r.sccResults {
		cmdsBuffer = cmdsBuffer[:0]
		for _, instance := range sccNodes {
			cmdsBuffer = append(cmdsBuffer, instance.cmds...)
		}
		// return results from state machine are not being used currently

		_, err := r.StateMachine.Execute(cmdsBuffer)
		if err != nil {
			return err
		}
		// TODO: transaction one SCC
		for _, instance := range sccNodes {
			instance.SetExecuted()
		}
	}
	return nil
}

// Assumption this function is based on:
// - If a node is executed, all SCC it belongs to or depending has been executed.
func (r *Replica) resolveConflicts(node *Instance) bool {
	if node == nil || !node.isAtStatus(committed) {
		panic("")
	}

	node.sccIndex = r.sccIndex
	node.sccLowlink = r.sccIndex
	r.sccIndex++

	r.pushSccStack(node)
	for iSpace := 0; iSpace < int(r.Size); iSpace++ {
		dep := node.deps[iSpace]
		if r.IsCheckpoint(dep) {
			continue
		}

		neighbor := r.InstanceMatrix[iSpace][dep]
		if !neighbor.isAtStatus(committed) {
			return false
		}

		if neighbor.isExecuted() {
			continue
		}

		if neighbor.sccIndex == 0 {
			if ok := r.resolveConflicts(neighbor); !ok {
				return false
			}
			if neighbor.sccLowlink < node.sccLowlink {
				node.sccLowlink = neighbor.sccLowlink
			}
		} else if r.inSccStack(neighbor) {
			if neighbor.sccIndex < node.sccLowlink {
				node.sccLowlink = neighbor.sccIndex
			}
		}
	}

	// found one SCC
	if node.sccLowlink == node.sccIndex {
		singleScc := make(sccNodesQueue, 0)
		for {
			n := r.popSccStack()
			singleScc = append(singleScc, n)
			if node == n {
				break
			}
		}
		sort.Sort(singleScc)
		r.sccResults = append(r.sccResults, singleScc)
	}
	return true
}

func (r *Replica) pushSccStack(i *Instance) {
	r.sccStack.PushBack(i)
}

// TODO: this could be optimized in O(1) with marking flag.
func (r *Replica) inSccStack(other *Instance) bool {
	iter := r.sccStack.Front()
	for iter != nil {
		self := iter.Value.(*Instance)
		if self == other {
			return true
		}
		iter = iter.Next()
	}
	return false
}

func (r *Replica) popSccStack() *Instance {
	res := r.sccStack.Back().Value.(*Instance)
	r.sccStack.Remove(r.sccStack.Back())
	return res
}

// interfaces for sorting sccResult
// sort by
// 1, rowId,
// 2, id,
// so, i < j if and only if
// - i.rowId < j.rowId or,
// - i.rowId == j.rowId && i.id < j.id
type sccNodesQueue []*Instance

func (s sccNodesQueue) Len() int      { return len(s) }
func (s sccNodesQueue) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sccNodesQueue) Less(i, j int) bool {
	if s[i].rowId < s[j].rowId {
		return true
	}
	if s[i].rowId == s[j].rowId {
		return s[i].id < s[j].id
	}
	return false
}
