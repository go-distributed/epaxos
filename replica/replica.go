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
	"bytes"
	"container/list"
	"encoding/gob"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/go-distributed/epaxos"
	"github.com/go-distributed/epaxos/message"
	"github.com/go-distributed/epaxos/persistent"
	"github.com/golang/glog"
)

// #if test
var v1Log = glog.V(0)
var v2Log = glog.V(0)

// #else
// var v1Log = glog.V(1)
// var v2Log = glog.V(2)
// #end

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
)

// actions
const (
	noAction uint8 = iota + 1
	replyAction
	fastQuorumAction
	broadcastAction
)

const (
	defaultClusterSize     = 3
	defaultCheckpointCycle = 1024
	defaultBatchInterval   = time.Millisecond * 50
	defaultTimeoutInterval = time.Millisecond * 50
	defaultExecuteInterval = time.Millisecond * 50
)

const defaultStartPort = 8080

// ****************************
// ***** TYPE STRUCT **********
// ****************************

type PackedReplica struct {
	Id             uint8
	Size           uint8
	MaxInstanceNum []uint64
	ExecutedUpTo   []uint64
	ProposeNum     uint64
}

type Replica struct {
	Id              uint8
	Size            uint8
	MaxInstanceNum  []uint64 // the highest instance number seen for each replica
	ProposeNum      uint64
	ProposeChan     chan *proposeRequest
	BatchInterval   time.Duration
	TimeoutInterval time.Duration

	CheckpointCycle uint64
	ExecutedUpTo    []uint64
	InstanceMatrix  [][]*Instance
	StateMachine    epaxos.StateMachine
	Epoch           uint32
	MessageChan     chan message.Message
	Addrs           []string
	Transporter     epaxos.Transporter

	// tarjan SCC
	sccStack   *list.List
	sccResults [][]*Instance
	sccIndex   int

	// tickers
	executeTicker *time.Ticker
	timeoutTicker *time.Ticker
	proposeTicker *time.Ticker

	// triggers
	executeTrigger chan bool

	// controllers
	enableBatching bool
	stop           chan struct{}

	// persistent store
	enablePersistent bool
	store            *persistent.LevelDB
}

type Param struct {
	ReplicaId        uint8
	Size             uint8
	StateMachine     epaxos.StateMachine
	CheckpointCycle  uint64
	BatchInterval    time.Duration
	TimeoutInterval  time.Duration
	ExecuteInterval  time.Duration
	Addrs            []string
	Transporter      epaxos.Transporter
	EnableBatching   bool
	EnablePersistent bool
	Restore          bool
	PersistentPath   string
}

type proposeRequest struct {
	cmds message.Commands
	id   chan uint64
}

func newProposeRequest(command ...message.Command) *proposeRequest {
	return &proposeRequest{
		cmds: message.Commands(command),
		id:   make(chan uint64, 1), // avoid blocking
	}
}

func verifyparam(param *Param) error {
	// TODO: replicaID, uuid
	if param.Size == 0 {
		param.Size = defaultClusterSize
	}
	if param.Size%2 == 0 {
		// TODO: epaxos replica error
		return fmt.Errorf("Use odd number as quorum size")
	}
	if param.CheckpointCycle == 0 {
		param.CheckpointCycle = defaultCheckpointCycle
	}
	if param.BatchInterval == 0 {
		param.BatchInterval = defaultBatchInterval
	}
	if param.TimeoutInterval == 0 {
		param.TimeoutInterval = defaultTimeoutInterval
	}
	if param.ExecuteInterval == 0 {
		param.ExecuteInterval = defaultExecuteInterval
	}
	if param.Addrs == nil {
		param.Addrs = make([]string, param.Size)
		for i := 0; i < int(param.Size); i++ {
			param.Addrs[i] = fmt.Sprintf("localhost:%d", defaultStartPort+i)
		}
	}
	if param.Transporter == nil {
		return fmt.Errorf("No specified transporter")
	}
	return nil
}

func New(param *Param) (*Replica, error) {
	err := verifyparam(param)
	if err != nil {
		return nil, err
	}

	r := &Replica{
		Id:              param.ReplicaId,
		Size:            param.Size,
		MaxInstanceNum:  make([]uint64, param.Size),
		ProposeNum:      1, // instance.id start from 1
		ProposeChan:     make(chan *proposeRequest, 1024),
		BatchInterval:   param.BatchInterval,
		TimeoutInterval: param.TimeoutInterval,
		CheckpointCycle: param.CheckpointCycle,
		ExecutedUpTo:    make([]uint64, param.Size),
		InstanceMatrix:  make([][]*Instance, param.Size),
		StateMachine:    param.StateMachine,
		Epoch:           epochStart,
		MessageChan:     make(chan message.Message, 1024),
		sccStack:        list.New(),
		Addrs:           param.Addrs,
		Transporter:     param.Transporter,

		executeTicker: time.NewTicker(param.ExecuteInterval),
		timeoutTicker: time.NewTicker(param.TimeoutInterval),
		proposeTicker: time.NewTicker(param.BatchInterval),

		executeTrigger:   make(chan bool),
		stop:             make(chan struct{}),
		enableBatching:   param.EnableBatching,
		enablePersistent: param.EnablePersistent,
	}

	var path string
	if param.PersistentPath == "" {
		path = fmt.Sprintf("%s-%d", "/dev/shm/test", r.Id)
	} else {
		path = param.PersistentPath
	}

	r.store, err = persistent.NewLevelDB(path, param.Restore)
	if err != nil {
		glog.Errorln("replica.New: failed to make new storage")
		return nil, err
	}

	for i := uint8(0); i < param.Size; i++ {
		r.InstanceMatrix[i] = make([]*Instance, defaultInstancesLength)
		r.MaxInstanceNum[i] = conflictNotFound
		r.ExecutedUpTo[i] = conflictNotFound
	}

	// restore replica and instances
	if param.Restore {
		err := r.RecoverFromPersistent()
		if err != nil {
			glog.Errorln("Recover from persistent failed!")
			return nil, err
		}
	}

	r.Transporter.RegisterChannel(r.MessageChan)

	if !r.enableBatching {
		// stop ticker
		r.proposeTicker.Stop()
	}

	return r, nil
}

// Start running the replica. It shouldn't stop at any time.
func (r *Replica) Start() error {
	go r.eventLoop()
	go r.executeLoop()
	go r.proposeLoop()
	go r.timeoutLoop()
	return r.Transporter.Start()
}

func (r *Replica) stopTickers() {
	r.executeTicker.Stop()
	r.timeoutTicker.Stop()
	r.proposeTicker.Stop()
}

func (r *Replica) Stop() {
	close(r.stop)
	r.stopTickers()
	r.Transporter.Stop()
	r.store.Close()
}

func (r *Replica) timeoutLoop() {
	for {
		select {
		case <-r.stop:
			return
		case <-r.timeoutTicker.C:
			r.checkTimeout()
		}
	}
}

func (r *Replica) checkTimeout() {
	for i, instance := range r.InstanceMatrix {

		// from executeupto to max, test timestamp,
		// if timeout, then send prepare
		for j := r.ExecutedUpTo[i] + 1; j <= r.MaxInstanceNum[i]; j++ {
			if r.IsCheckpoint(j) { // [*]Note: the first instance is also a checkpoint
				continue
			}
			if instance[j] == nil || instance[j].isTimeout() {
				r.MessageChan <- r.makeTimeout(uint8(i), j)
			}
		}
	}
}

func (r *Replica) makeTimeout(rowId uint8, instanceId uint64) message.Message {
	return &message.Timeout{
		ReplicaId:  rowId,
		InstanceId: instanceId,
		From:       r.Id,
	}
}

// handling events
// TODO: differentiate internal and external messages
func (r *Replica) eventLoop() {
	for {
		select {
		case <-r.stop:
			return
		case msg := <-r.MessageChan:
			r.dispatch(msg)
		}
	}
}

func (r *Replica) executeLoop() {
	for {
		select {
		case <-r.stop:
			return
		case <-r.executeTicker.C:
			// execution of committed instances
			r.findAndExecute()
		case <-r.executeTrigger:
			r.findAndExecute()
		}
	}
}

func (r *Replica) proposeLoop() {
	bufferedRequests := make([]*proposeRequest, 0) // start from 0

	for {
		select {
		case <-r.stop:
			return
		case req := <-r.ProposeChan:
			bufferedRequests = append(bufferedRequests, req)
			if !r.enableBatching {
				r.BatchPropose(&bufferedRequests)
			}
		case <-r.proposeTicker.C:
			r.BatchPropose(&bufferedRequests)
		}
	}
}

// return the channel containing the internal instance id
func (r *Replica) Propose(cmds ...message.Command) chan uint64 {
	req := newProposeRequest(cmds...)
	r.ProposeChan <- req
	return req.id
}

// TODO: This must be done in a synchronized/atomic way.
func (r *Replica) BatchPropose(batchedRequests *[]*proposeRequest) {
	defer func() { *batchedRequests = (*batchedRequests)[:0] }() // resize

	br := *batchedRequests
	if len(br) == 0 {
		return
	}

	// copy commands
	cmds := make([]message.Command, 0)
	for i := range br {
		cmds = append(cmds, br[i].cmds...)
	}

	// record the current instance id
	iid := r.ProposeNum
	proposal := message.NewPropose(r.Id, iid, cmds)

	// update propose num
	r.ProposeNum++
	if r.IsCheckpoint(r.ProposeNum) {
		r.ProposeNum++
	}
	r.StoreReplica()

	// TODO: we could use another channel
	// and with synchronization to improve throughput
	r.MessageChan <- proposal

	// wait for the construction of instance and
	// send back its id
	<-proposal.Created
	for _, req := range br {
		req.id <- iid
		close(req.id)
	}
}

// *****************************
// ***** Message Handling ******
// *****************************

// This function is responsible for communicating with instance processing.
func (r *Replica) dispatch(msg message.Message) {
	replicaId := msg.Replica()
	instanceId := msg.Instance()

	r.updateMaxInstanceNum(replicaId, instanceId)

	v1Log.Infof("Replica[%v]: recv message[%s], from Replica[%v]\n",
		r.Id, msg.String(), msg.Sender())
	if glog.V(0) {
		printDependencies(msg)
	}

	if instanceId <= conflictNotFound {
		panic("")
	}

	if r.InstanceMatrix[replicaId][instanceId] == nil {
		r.InstanceMatrix[replicaId][instanceId] = NewInstance(r, replicaId, instanceId)
		if p, ok := msg.(*message.Propose); ok {
			// send back a signal for this successfully creation
			close(p.Created)
		}
	}

	i := r.InstanceMatrix[replicaId][instanceId]
	i.touch() // update last touched timestamp

	v1Log.Infof("Replica[%v]: instance[%v][%v] status before = %v, ballot = [%v]\n",
		r.Id, replicaId, instanceId, i.StatusString(), i.ballot.String())
	v2Log.Infof("dependencies before: %v\n", i.Dependencies())

	var action uint8
	var rep message.Message

	switch i.status {
	case nilStatus:
		action, rep = i.nilStatusProcess(msg)
	case preAccepted:
		action, rep = i.preAcceptedProcess(msg)
	case accepted:
		action, rep = i.acceptedProcess(msg)
	case committed:
		action, rep = i.committedProcess(msg)
	case preparing:
		action, rep = i.preparingProcess(msg)
	default:
		panic("")
	}

	v2Log.Infof("dependencies after: %v\n", i.Dependencies())
	if action == noAction {
		v1Log.Infof("Replica[%v]: instance[%v][%v] status after = %v, ballot = [%v]\n\n\n",
			r.Id, replicaId, instanceId, i.StatusString(), i.ballot.String())
	} else {
		v1Log.Infof("Replica[%v]: instance[%v][%v] status after = %v, ballot = [%v]\n",
			r.Id, replicaId, instanceId, i.StatusString(), i.ballot.String())
	}

	if r.enablePersistent {
		r.StoreSingleInstance(i)
	}

	switch action {
	case noAction:
		return
	case replyAction:
		v1Log.Infof("Replica[%v]: send message[%s], to Replica[%v]\n\n\n",
			r.Id, rep.String(), msg.Sender())
		r.Transporter.Send(msg.Sender(), rep) // send back to the sender of the message
	case fastQuorumAction:
		v1Log.Infof("Replica[%v]: send message[%s], to FastQuorum\n\n\n",
			r.Id, rep.String())
		r.Transporter.MulticastFastquorum(rep)
	case broadcastAction:
		v1Log.Infof("Replica[%v]: send message[%s], to Everyone\n\n\n",
			r.Id, rep.String())
		r.Transporter.Broadcast(rep)
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

func (r *Replica) makeInitialBallot() *message.Ballot {
	return message.NewBallot(r.Epoch, 0, r.Id)
}

func (r *Replica) makeInitialDeps() message.Dependencies {
	return make(message.Dependencies, r.Size)
}

// This func initiate a new instance, construct its commands and dependencies
// TODO: this operation should synchronized/atomic
func (r *Replica) initInstance(cmds message.Commands, i *Instance) {
	if i.rowId != r.Id {
		panic("")
	}

	deps := make(message.Dependencies, r.Size)

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
	i.cmds, i.deps = cmds.Clone(), deps.Clone()
	// we can only update here because
	// now we are safe to have cmds, etc. inside instance
	//if !i.replica.updateMaxInstanceNum(i.rowId, i.id) {
	//	panic("")
	//}
	i.replica.updateMaxInstanceNum(i.rowId, i.id)
}

// This func updates the passed in dependencies from replica[from].
// return updated dependencies and whether the dependencies has changed.
// TODO: this operation should synchronized/atomic
func (r *Replica) updateInstance(cmds message.Commands, deps message.Dependencies, from uint8, i *Instance) bool {
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
func (r *Replica) scanConflicts(instances []*Instance, cmds message.Commands, start uint64, end uint64) uint64 {
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
	if r.enablePersistent {
		defer r.StoreReplica()
	}
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

			// [*] if the instance is nil, then we should not continue to execute,
			// because this instance maybe already commited and executed by other
			// replicas
			if instance == nil {
				break
			}
			if !instance.isAtStatus(committed) {
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

	v2Log.Infoln("start resolve")
	if ok := r.resolveConflicts(i); !ok {
		v2Log.Infoln("there is incomplete scc")
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
	cmdsBuffer := make([]message.Command, 0)

	// batch all commands in the scc
	v2Log.Infoln("execute list")
	for _, sccNodes := range r.sccResults {
		v2Log.Infoln("one scc")
		for _, instance := range sccNodes {
			v2Log.Infof("Instance [%v][%v] executed\n", instance.rowId, instance.id)
		}
		v2Log.Infoln("scc end")
		//v2Log.Infoln()

		cmdsBuffer = cmdsBuffer[:0]
		for _, instance := range sccNodes {
			cmdsBuffer = append(cmdsBuffer, instance.cmds...)
		}
		// return results from state machine are not being used currently

		// TODO: the results from statemachine should be relayed to callback
		//      of some client function
		_, err := r.StateMachine.Execute(cmdsBuffer)
		if err != nil {
			return err
		}
		// TODO: transaction one
		for _, instance := range sccNodes {
			instance.SetExecuted()
		}
		if r.enablePersistent {
			r.StoreInstances(sccNodes...)
		}
	}
	return nil
}

// Assumption this function is based on:
// - If a node is executed, all SCC it belongs to or depending has been executed.
func (r *Replica) resolveConflicts(node *Instance) bool {
	v2Log.Infof("resolve for [%v][%v]\n", node.rowId, node.id)
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
		if neighbor == nil || !neighbor.isAtStatus(committed) {
			r.clearStack()
			return false
		}

		if neighbor.isExecuted() {
			continue
		}

		if neighbor.sccIndex == 0 {
			if ok := r.resolveConflicts(neighbor); !ok {
				r.clearStack()
				return false
			}
			if neighbor.sccLowlink < node.sccLowlink {
				node.sccLowlink = neighbor.sccLowlink
			}
		} else if r.inSccStack(neighbor) {
			if neighbor.sccLowlink < node.sccLowlink {
				node.sccLowlink = neighbor.sccLowlink
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
	v2Log.Infof("push stack [%v][%v]\n", i.rowId, i.id)
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
	v2Log.Infof("pop stack [%v][%v]\n", res.rowId, res.id)
	return res
}

func (r *Replica) clearStack() {
	iter := r.sccStack.Front()
	for iter != nil {
		instance := iter.Value.(*Instance)
		v2Log.Infof("clear [%v][%v]\n", instance.rowId, instance.id)
		instance.sccIndex = 0
		instance.sccLowlink = 0
		iter = iter.Next()
	}
	r.sccStack.Init()
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

func printDependencies(msg message.Message) {
	switch m := msg.(type) {
	case *message.PreAccept:
		v2Log.Infof("dependencies: %v\n", m.Deps)
	case *message.PreAcceptReply:
		v2Log.Infof("dependencies: %v\n", m.Deps)
	case *message.Accept:
		v2Log.Infof("dependencies: %v\n", m.Deps)
	case *message.PrepareReply:
		v2Log.Infof("dependencies: %v\n", m.Deps)
	case *message.Commit:
		v2Log.Infof("dependencies: %v\n", m.Deps)
	}
}

// store and restore the instance
func (r *Replica) StoreSingleInstance(inst *Instance) error {
	var buffer bytes.Buffer

	p := inst.Pack()
	key := fmt.Sprintf("%v-%v-%v", r.Id, p.RowId, p.Id)
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(p)
	if err != nil {
		return err
	}
	return r.store.Put(key, buffer.Bytes())
}

func (r *Replica) RestoreSingleInstance(rowId uint8, instanceId uint64) (*Instance, error) {
	inst := NewInstance(r, rowId, instanceId)
	var p PackedInstance

	key := fmt.Sprintf("%v-%v-%v", r.Id, inst.rowId, inst.id)
	b, err := r.store.Get(key)
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(&p)
	if err != nil {
		return nil, err
	}
	inst.Unpack(&p)
	return inst, nil
}

func (r *Replica) StoreInstances(insts ...*Instance) error {
	kvs := make([]*epaxos.KVpair, len(insts))
	for i := range insts {
		var buffer bytes.Buffer
		p := insts[i].Pack()
		key := fmt.Sprintf("%v-%v-%v", r.Id, insts[i].rowId, insts[i].id)
		enc := gob.NewEncoder(&buffer)
		err := enc.Encode(p)
		if err != nil {
			return err
		}
		kvs[i] = &epaxos.KVpair{
			Key:   key,
			Value: buffer.Bytes(),
		}
	}
	return r.store.BatchPut(kvs)
}

// pack and unpack the replica
func (r *Replica) Pack() *PackedReplica {
	p := &PackedReplica{
		Id:             r.Id,
		Size:           r.Size,
		MaxInstanceNum: make([]uint64, r.Size),
		ExecutedUpTo:   make([]uint64, r.Size),
		ProposeNum:     r.ProposeNum,
	}
	for i := uint8(0); i < r.Size; i++ {
		p.MaxInstanceNum[i] = r.MaxInstanceNum[i]
		p.ExecutedUpTo[i] = r.ExecutedUpTo[i]
	}
	return p
}

func (r *Replica) Unpack(p *PackedReplica) {
	r.Id = p.Id
	r.Size = p.Size
	for i := uint8(0); i < r.Size; i++ {
		r.MaxInstanceNum[i] = p.MaxInstanceNum[i]
		r.ExecutedUpTo[i] = p.ExecutedUpTo[i]
	}
	r.ProposeNum = p.ProposeNum
}

// store and restore the replica
func (r *Replica) StoreReplica() error {
	var buffer bytes.Buffer

	p := r.Pack()
	key := fmt.Sprintf("%v-replica", r.Id)
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(p)
	if err != nil {
		return err
	}
	return r.store.Put(key, buffer.Bytes())
}

func (r *Replica) RestoreReplica() error {
	var p PackedReplica

	key := fmt.Sprintf("%v-replica", r.Id)
	b, err := r.store.Get(key)
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(&p)
	r.Unpack(&p)

	return nil
}

// recover from persistent storage
func (r *Replica) RecoverFromPersistent() error {
	err := r.RestoreReplica()
	if err != nil {
		glog.Errorln("replica.New: failed to restore replica info")
		return err
	}

	for i := uint8(0); i < r.Size; i++ {
		for j := uint64(0); j <= r.MaxInstanceNum[i]; j++ {
			if r.IsCheckpoint(j) {
				continue
			}
			inst, err := r.RestoreSingleInstance(i, j)
			if err != nil && err != epaxos.ErrorNotFound {
				glog.Errorf("replica.New: failed to restore instance info, for [%v][%v]\n", i, j)
				return err
			}
			r.InstanceMatrix[i][j] = inst
		}
	}
	return nil
}
