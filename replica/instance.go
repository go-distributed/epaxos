package replica

// This file implements instance module.
//
// We are listing here the assumptions made in our implementaion.
// @assumption:
// - When a replica pass in the message to instance methods, we assume that the
//   internal fields of message is readable only and safe to reference to, except deps.
// @assumption(02/01/14):
// - When a new instance is created, it's at nilstatus.
// @decision (01/31/14):
// - Status has precedence. An accepted instance won't handle pre-accept even if
//   the pre-accept carries larger ballot.
// - nilstatus < pre-accepted < accepted < committed
// @decision (02/01/14):
// - Executed won't be included in instance statuses anymore.
// - Executed will be recorded in a flag. This will simplify the state machine.
// @decision (02/05/14):
// - No-op == Commands(nil)
// @assumption (02/10/14):
// - In initial round, replica will broadcast preaccept to fast quorum.
// - In later rounds, replica will broadcast to all from preparing.
// @assumption (04/17/14):
// - The initial ballot is (epoch, 0, replicaId)
// @decision (04/18/14):
// - Delete rejections to avoid complexity

import (
	"fmt"
	"time"

	"github.com/sargun/epaxos/message"
)

// ****************************
// *****  CONST ENUM **********
// ****************************

// instance status
const (
	nilStatus uint8 = iota + 1
	preparing
	preAccepted
	accepted
	committed
)

// ****************************
// ***** TYPE STRUCT **********
// ****************************

type Instance struct {
	cmds        message.Commands
	deps        message.Dependencies
	status      uint8
	ballot      *message.Ballot
	lastTouched time.Time

	info         *InstanceInfo
	recoveryInfo *RecoveryInfo

	// local information
	replica  *Replica
	rowId    uint8
	id       uint64
	executed bool

	// tarjan SCC
	sccIndex   int
	sccLowlink int

	CommittedNotify chan struct{}
	ExecutedNotify  chan struct{}
}

// bookkeeping struct for recording counts of different messages and some flags
type InstanceInfo struct {
	samePreAcceptReplies bool

	preAcceptOkCount    int
	preAcceptReplyCount int
	acceptReplyCount    int
}

// recovery info will keep information of the instance info that we will send out on
// the next stage.
type RecoveryInfo struct {
	// This is used to store the identical non original leader pre-accept replies.
	identicalCount int

	replyCount int
	ballot     *message.Ballot // for book keeping

	cmds         message.Commands
	deps         message.Dependencies
	status       uint8
	formerStatus uint8
}

type PackedRecoveryInfo struct {
	Ballot       *message.Ballot
	Cmds         message.Commands
	Deps         message.Dependencies
	Status       uint8
	FormerStatus uint8
}

// This is for marshal/unmarshaling the instance
type PackedInstance struct {
	Cmds               message.Commands
	Deps               message.Dependencies
	Status             uint8
	Ballot             *message.Ballot
	RowId              uint8
	Id                 uint64
	Executed           bool
	PackedRecoveryInfo *PackedRecoveryInfo
}

// ****************************
// **** NEW INSTANCE **********
// ****************************

func NewInstance(replica *Replica, rowId uint8, instanceId uint64) (i *Instance) {
	i = &Instance{
		replica:      replica,
		id:           instanceId,
		rowId:        rowId,
		deps:         replica.makeInitialDeps(),
		info:         NewInstanceInfo(),
		recoveryInfo: NewRecoveryInfo(),
		ballot:       message.NewBallot(0, 0, 0),
		status:       nilStatus,

		CommittedNotify: make(chan struct{}),
		ExecutedNotify:  make(chan struct{}),
	}
	return i
}

func NewInstanceInfo() *InstanceInfo {
	return &InstanceInfo{
		samePreAcceptReplies: true,
	}
}

func NewRecoveryInfo() *RecoveryInfo {
	return &RecoveryInfo{
		ballot: message.NewBallot(0, 0, 0),
	}
}

// *********************************
// ******** INSTANCE FIELDS  *******
// *********************************
func (i *Instance) isAtStatus(status uint8) bool {
	return i.status == status
}

func (i *Instance) isAfterStatus(status uint8) bool {
	return i.status > status
}

func (i *Instance) isAtOrAfterStatus(status uint8) bool {
	return i.status >= status
}

func (i *Instance) isBeforeStatus(status uint8) bool {
	return i.status < status
}

func (i *Instance) isNewBorn() bool {
	return i.ballot.GetEpoch() == 0
}

func (i *Instance) Commands() message.Commands {
	return i.cmds
}

func (i *Instance) Dependencies() message.Dependencies {
	return i.deps
}

func (i *InstanceInfo) reset() {
	i.samePreAcceptReplies = true
	i.preAcceptOkCount = 0
	i.preAcceptReplyCount = 0
	i.acceptReplyCount = 0
}

func (i *Instance) initRecoveryInfo() {
	ir := i.recoveryInfo

	ir.replyCount = 0
	ir.cmds = i.cmds.Clone()
	ir.deps = i.deps.Clone()

	if i.isAtStatus(preparing) {
		// from preparing to preparing, we should extend the former status
		ir.status = ir.formerStatus
	} else {
		ir.status = i.status
	}

	if i.status != preparing {
		// from preparing to preparing, we should keep the former status
		ir.formerStatus = i.status
	}
	ir.ballot = i.ballot.Clone()

	// preacceptcount is used to count N/2 identical initial preaccepts.
	if i.isAtStatus(preAccepted) && i.ballot.IsInitialBallot() && i.rowId != i.replica.Id {
		i.recoveryInfo.identicalCount = 1
	} else {
		i.recoveryInfo.identicalCount = 0
	}
}

func (r *RecoveryInfo) statusIsBefore(status uint8) bool {
	return r.status < status
}

func (r *RecoveryInfo) statusIs(status uint8) bool {
	return r.status == status
}

func (r *RecoveryInfo) statusIsAfter(status uint8) bool {
	return r.status > status
}

func (r *RecoveryInfo) updateByPrepareReply(p *message.PrepareReply) {
	r.cmds, r.deps = p.Cmds.Clone(), p.Deps.Clone()
	r.ballot, r.status = p.OriginalBallot.Clone(), p.Status
}

// ******************************
// ****** State Processing ******
// ******************************

// NilStatus exists for:
// - the instance is newly created when
// - - received a proposal first time and only once. (sender)
// - - received pre-accept, accept, commit, prepare the first time. (receiver)
// - - required by commit dependencies and transitioning to preparing. (sender)
// - the instance is not newly created when
// - - after reverted back from `preparing`(sender -> receiver)
// - - received prepare and waiting for further message. (receiver)
func (i *Instance) nilStatusProcess(m message.Message) (action uint8, msg message.Message) {
	defer i.checkStatus(nilStatus, preAccepted, accepted, committed, preparing)

	if !i.isAtStatus(nilStatus) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *message.Propose:
		return i.handlePropose(content)
	case *message.PreAccept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handlePreAccept(content)
	case *message.Accept:
		if content.Ballot.Compare(i.ballot) < 0 {
			// [*] this could happens when the instance revert from preparing
			return noAction, nil
		}
		return i.handleAccept(content)
	case *message.Commit:
		return i.handleCommit(content)
	case *message.Timeout:
		return i.handleTimeout(content)
	case *message.Prepare:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handlePrepare(content)
	case *message.PrepareReply:
		if i.isNewBorn() || i.ballot.GetNumber() == 0 {
			panic("Never send prepare before but receive prepare reply")
		}
		return noAction, nil
	case *message.PreAcceptReply, *message.AcceptReply, *message.PreAcceptOk:
		panic("")
	default:
		panic("")
	}
}

// preaccepted instance
// - handles preaccept-ok/-reply, preaccept, accept, commit, and prepare.
func (i *Instance) preAcceptedProcess(m message.Message) (action uint8, msg message.Message) {
	defer i.checkStatus(preAccepted, accepted, committed, preparing)

	if !i.isAtStatus(preAccepted) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *message.PreAccept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handlePreAccept(content)
	case *message.Accept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handleAccept(content)
	case *message.Commit:
		return i.handleCommit(content)
	case *message.Timeout:
		return i.handleTimeout(content)
	case *message.Prepare:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handlePrepare(content)
	case *message.PreAcceptReply:
		if content.Ballot.Compare(i.ballot) < 0 {
			// ignore stale PreAcceptReply
			return noAction, nil
		}
		return i.handlePreAcceptReply(content)
	case *message.PreAcceptOk:
		if !i.ballot.IsInitialBallot() {
			return noAction, nil // ignore stale reply
		}
		return i.handlePreAcceptOk(content)
	case *message.AcceptReply:
		panic("")
	case *message.PrepareReply:
		if i.ballot.IsInitialBallot() {
			panic("")
		}
		return noAction, nil
	default:
		panic("")
	}
}

// accepted instance can be of two roles:
// - as a sender
// - - It will handle corresponding accept reply. On majority votes, it will
// - - transition to committed and broadcast commit.
// - as a receiver
// - - It will handle accept, prepare with larger ballot, and commit.
func (i *Instance) acceptedProcess(m message.Message) (action uint8, msg message.Message) {
	defer i.checkStatus(accepted, committed, preparing)

	if !i.isAtStatus(accepted) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *message.PreAccept:
		return noAction, nil
	case *message.Accept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handleAccept(content)
	case *message.Commit:
		return i.handleCommit(content)
	case *message.Timeout:
		return i.handleTimeout(content)
	case *message.Prepare:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handlePrepare(content)
	case *message.AcceptReply:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil // ignore stale PreAcceptReply
		}
		return i.handleAcceptReply(content)
	case *message.PreAcceptReply, *message.PreAcceptOk:
		return noAction, nil // ignore stale replies
	case *message.PrepareReply:
		if i.ballot.IsInitialBallot() {
			panic("")
		}
		return noAction, nil // ignore stale replies
	default:
		panic("")
	}
}

// committed instance will
// - reject all request messages (pre-accept, accept),
// - handle prepare to help it find committed,
// - ignore others
func (i *Instance) committedProcess(m message.Message) (action uint8, msg message.Message) {
	defer i.checkStatus(committed)

	if !i.isAtStatus(committed) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *message.PreAccept:
		return noAction, nil
	case *message.Accept:
		return noAction, nil
	case *message.Timeout:
		// here we ignore the timeout event instead of panic,
		// because sometimes timeout event
		// comes right after the instance becomes committed
		return noAction, nil
	case *message.Prepare:
		return i.handlePrepare(content)
	case *message.PreAcceptReply, *message.PreAcceptOk, *message.AcceptReply, *message.PrepareReply, *message.Commit:
		return noAction, nil // ignore stale replies
	default:
		panic("")
	}
}

// preparing instance could only acts as a sender.
// It handles most kinds of messages (in some conditions with larger ballot) and
// ignores all replies except prepare reply.
func (i *Instance) preparingProcess(m message.Message) (action uint8, msg message.Message) {
	defer i.checkStatus(preparing, preAccepted, accepted, committed, nilStatus)

	if !i.isAtStatus(preparing) || i.recoveryInfo == nil {
		panic("")
	}

	switch content := m.Content().(type) {
	case *message.PreAccept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handlePreAccept(content)
	case *message.Accept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handleAccept(content)
	case *message.Commit:
		return i.handleCommit(content)
	case *message.Timeout:
		return i.handleTimeout(content)
	case *message.Prepare:
		// the instance itself is the first one to have ballot of this
		// magnitude. It can't receive others having the same
		if content.Ballot.Compare(i.ballot) == 0 {
			panic("")
		}
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.revertAndHandlePrepare(content)
	case *message.PrepareReply:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handlePrepareReply(content)
	case *message.PreAcceptReply:
		if i.recoveryInfo.formerStatus < preAccepted {
			panic("")
		}
		return noAction, nil
	case *message.PreAcceptOk:
		if i.recoveryInfo.formerStatus < preAccepted {
			panic("")
		}
		return noAction, nil
	case *message.AcceptReply:
		if i.recoveryInfo.formerStatus < accepted {
			panic("")
		}
		// ignore delayed replies
		return noAction, nil
	default:
		panic("")
	}
}

// ******************************
// ****** Handle Message  *******
// ******************************

// a propose will broadcasted to fast quorum in pre-accept message.
func (i *Instance) handlePropose(p *message.Propose) (action uint8, msg *message.PreAccept) {
	if p.Cmds == nil || !i.isNewBorn() || !i.isAtStatus(nilStatus) {
		panic("")
	}

	i.ballot = i.replica.makeInitialBallot()
	i.enterPreAcceptedAsSender()
	i.replica.initInstance(p.Cmds, i)

	return fastQuorumAction, i.makePreAccept()
}

// When handling pre-accept, instance will set its ballot to newer one, and
// update deps if any change's found.
// Reply: pre-accept-OK if no change in deps; otherwise a normal pre-accept-reply.
// The pre-accept-OK contains just one field, which is a big optimization for serilization
func (i *Instance) handlePreAccept(p *message.PreAccept) (action uint8, msg message.Message) {
	if p.Ballot.Compare(i.ballot) < 0 {
		panic("")
	}

	i.enterPreAcceptedAsReceiver()
	i.ballot = p.Ballot.Clone()
	changed := i.replica.updateInstance(p.Cmds, p.Deps, i.rowId, i)

	if changed {
		return replyAction, i.makePreAcceptReply(true, i.deps)
	}

	// not initial leader
	if !p.Ballot.IsInitialBallot() {
		return replyAction, i.makePreAcceptReply(true, i.deps)
	}
	// pre-accept-ok for possible fast quorum commit
	return replyAction, &message.PreAcceptOk{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		From:       i.replica.Id,
	}
}

// This is used to check when handling preaccept-reply messages,
// can this instance could still go to fast path
//
// we are not able to commit on past path if the following happens
//
// 1, not all replies are the same
// 2, we are not in the initial round.
//
// And for condition 1, we have two cases:
//
// a, not all preAcceptReplies are the same
// b, we have received both preAcceptReplies and preAcceptOks
func (i *Instance) notAbleToFastPath() bool {
	return !i.info.samePreAcceptReplies ||
		!i.ballot.IsInitialBallot() ||
		(i.info.preAcceptOkCount > 0 && i.info.preAcceptReplyCount > 0)
}

func (i *Instance) ableToFastPath() bool {
	return !i.notAbleToFastPath()
}

// common routine for judging next step for handle preaccept-ok and -reply
// The requirement for fast path is:
// - all deps in replies are the same.
// - initial ballot (first round from propose)
func (i *Instance) commonPreAcceptedNextStep() (action uint8, msg message.Message) {
	replyCount := i.info.preAcceptReplyCount
	okCount := i.info.preAcceptOkCount

	// fast path, we received fast quourm of
	// - all preaccept-ok
	// - all preaccept-reply same (implied in abletofastpath())
	// and they satisfy fast path (initial ballot)
	if okCount == i.replica.fastQuorum() ||
		replyCount == i.replica.fastQuorum() && i.ableToFastPath() {
		// TODO: persistent
		i.enterCommitted()
		return broadcastAction, i.makeCommit()
	}

	// slow path, we received quorum of
	// - replies and they don't satisfy fast path.
	// - a mix of preacept-ok/-reply implies different and deps.
	if okCount+replyCount >= i.replica.quorum() && !i.ableToFastPath() {
		// TODO: persistent
		i.enterAcceptedAsSender()
		return broadcastAction, i.makeAccept()
	}

	return noAction, nil
}

// handlePreAcceptOk handles PreAcceptOks,
// One replica will receive PreAcceptOks only if it's the initial leader,
// On receiving this message,
// it will increment the preAcceptReplyCount, and if
// - preAcceptReplyCount >= the size of fast quorum, and all replies are
//   the same, then it will broadcast Commits,
// - preAcceptReplyCount >= N/2(not including the sender), and not all replies are equal,
//   then it will broadcast Accepts,
// - otherwise, do nothing
func (i *Instance) handlePreAcceptOk(p *message.PreAcceptOk) (action uint8, msg message.Message) {
	if !i.ballot.IsInitialBallot() {
		panic("")
	}

	// we would only send out initial pre-accepts to fast quorum
	if i.info.preAcceptOkCount == i.replica.fastQuorum() {
		panic("")
	}

	i.info.preAcceptOkCount++

	return i.commonPreAcceptedNextStep()
}

// handlePreAcceptReply:
// receiving negative pre-accept reply (ok == false), for someone has larger ballot
// - update ballot
// - instance becomes a receiver from a sender
// receiving corresponding pre-accept reply (ok == true)
// - union deps, and update counts
// Broadcast cases:
// - receiving == fast quorum replies with same deps
//    and the instance itself is initial leader,
//    then broadcast Commit (fast path)
// - receiving >= N/2 replies (not including the sender itself),
//    and not satisfying above condition,
//    then broadcast Accepts (slow path, Paxos Accept Phase)
// - Otherwise do nothing.
func (i *Instance) handlePreAcceptReply(p *message.PreAcceptReply) (action uint8, msg message.Message) {
	if !i.isSender() {
		return noAction, nil
	}

	if !i.isAtStatus(preAccepted) {
		panic("")
	}

	if p.Ballot.Compare(i.ballot) != 0 {
		panic("")
	}

	i.info.preAcceptReplyCount++

	// deps = union(deps from all replies)
	if same := i.deps.Union(p.Deps); !same {
		// We take difference of deps only for replies from other replica.
		if i.info.preAcceptReplyCount > 1 {
			i.info.samePreAcceptReplies = false
		}
	}

	return i.commonPreAcceptedNextStep()
}

// handleAccept handles Accept messages (receiver)
// Update:
// - cmds, ballot
// action: reply an AcceptReply message, with:
// - Ok = true
// - everything else = instance's fields
func (i *Instance) handleAccept(a *message.Accept) (action uint8, msg *message.AcceptReply) {
	if a.Ballot.Compare(i.ballot) < 0 {
		panic("")
	}

	if i.isAfterStatus(accepted) {
		panic("")
	}

	i.cmds, i.deps, i.ballot = a.Cmds.Clone(), a.Deps.Clone(), a.Ballot.Clone()
	i.enterAcceptedAsReceiver()

	return replyAction, i.makeAcceptReply(true)
}

// handleAcceptReply handles AcceptReplies as sender,
// Update:
// - ballot
// Broadcast event happens when:
// if receiving majority replies with ok == true,
//    then broadcast Commit
// otherwise: do nothing.
func (i *Instance) handleAcceptReply(a *message.AcceptReply) (action uint8, msg message.Message) {
	if !i.isSender() {
		return noAction, nil
	}

	if !i.isAtStatus(accepted) {
		panic("")
	}

	if a.Ballot.Compare(i.ballot) != 0 {
		panic("")
	}

	if i.info.acceptReplyCount == i.replica.quorum() {
		panic("")
	}

	i.info.acceptReplyCount++
	if i.info.acceptReplyCount >= i.replica.quorum() {
		i.enterCommitted()
		return broadcastAction, i.makeCommit()
	}

	return noAction, nil
}

// TODO: need testing
func (i *Instance) handleCommit(c *message.Commit) (action uint8, msg message.Message) {
	if i.isAtOrAfterStatus(committed) {
		panic("")
	}

	i.cmds, i.deps = c.Cmds.Clone(), c.Deps.Clone()
	i.enterCommitted()

	// TODO: Do we need to clear unnecessary objects to save more memory?
	// TODO: persistent
	return noAction, nil
}

func (i *Instance) revertAndHandlePrepare(p *message.Prepare) (action uint8, msg *message.PrepareReply) {
	i.checkStatus(preparing)
	i.status = i.recoveryInfo.formerStatus
	return i.handlePrepare(p)
}

func (i *Instance) handleTimeout(p *message.Timeout) (action uint8, msg *message.Prepare) {
	i.enterPreparing()
	return broadcastAction, i.makePrepare()
}

func (i *Instance) handlePrepare(p *message.Prepare) (action uint8, msg *message.PrepareReply) {
	oldBallot := i.ballot.Clone()

	// if the instance is alreay a commited one,
	// then it just replies with ok == true,
	// and does not update its ballot, so only
	// non-committed instance will need to update self ballot.
	if !i.isAtStatus(committed) {
		if p.Ballot.Compare(i.ballot) <= 0 { // cannot be equal or smaller
			panic(fmt.Sprintln("prepare ballot: ", p.Ballot, i.ballot))
		}
		i.ballot = p.Ballot.Clone()
	}

	isFromLeader := false
	if i.replica.Id == i.rowId {
		isFromLeader = true
	}

	return replyAction, &message.PrepareReply{
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Status:         i.status,
		Cmds:           i.cmds.Clone(),
		Deps:           i.deps.Clone(),
		Ballot:         p.Ballot.Clone(),
		OriginalBallot: oldBallot,
		IsFromLeader:   isFromLeader,
		From:           i.replica.Id,
	}
}

// This function handles the prepare reply and update recover info:
// - committed reply
// - accepted reply
// - pre-accepted reply
// - nilstatus (noop) reply
// Broadcast action happesn when quorum replies are received, and
// -
// Assumption:
// 1. Receive only first N/2 replies.
//   Even if we broadcast prepare to all, we only handle the first N/2 (positive)
//   replies.
//
// If a preparing instance as nilstatus handles prepare reply of
// - committed, it should set its recovery info according to the reply
// - accepted, it should set its recovery info according to the reply
// - pre-accepted, it should set its recovery info according to the reply
// - nilstatus, ignore
//
// If a preparing instance as preaccepted handles prepare reply of
// - committed, it should set its recovery info according to the reply
// - accepted, it should set its recovery info according to the reply
// - nilstatus, ignore
// - pre-accepted, where original ballot compared to recovery ballot is
// - - larger, set accordingly
// - - smaller, ignore
// - - same, but
// - - - not initial or initial but from leader, ignore
// Finally, send N/2 identical initial pre-accepted back, it should
// broadcast accepts.
func (i *Instance) handlePrepareReply(p *message.PrepareReply) (action uint8, msg message.Message) {
	if !i.isAtStatus(preparing) {
		panic("")
	}

	if p.Ballot.Compare(i.ballot) != 0 {
		panic("")
	}

	if i.recoveryInfo.replyCount >= i.replica.quorum() {
		panic("")
	}

	i.updateRecoveryInstance(p)

	i.recoveryInfo.replyCount++
	if i.recoveryInfo.replyCount >= i.replica.quorum() {
		return i.makeRecoveryDecision()
	}

	// We will wait until having received N/2 replies for next transition.

	return noAction, nil
}

// ***********************************
// ***** PREPAREREPLY HELPER *********
// ***********************************

func (i *Instance) updateRecoveryInstance(p *message.PrepareReply) {
	switch p.Status {
	case committed:
		i.handleCommittedPrepareReply(p)
	case accepted:
		i.handleAcceptedPrepareReply(p)
	case preAccepted:
		i.handlePreAcceptedPrepareReply(p)
	case nilStatus:
	default:
		panic("")
	}
}

func (i *Instance) handleCommittedPrepareReply(p *message.PrepareReply) {
	if i.recoveryInfo.statusIs(committed) {
		return
	}
	i.recoveryInfo.updateByPrepareReply(p)
}

func (i *Instance) handleAcceptedPrepareReply(p *message.PrepareReply) {
	ir := i.recoveryInfo
	if ir.statusIsAfter(accepted) {
		return
	}

	if ir.statusIsBefore(accepted) {
		ir.updateByPrepareReply(p)
		return
	}

	// for same status accepted reply, we will keep the one of largest ballot.
	if ir.ballot.Compare(p.OriginalBallot) >= 0 {
		return
	}

	ir.updateByPrepareReply(p)
}

func (i *Instance) handlePreAcceptedPrepareReply(p *message.PrepareReply) {
	ir := i.recoveryInfo
	if ir.statusIsAfter(preAccepted) {
		return
	}

	if ir.statusIsBefore(preAccepted) {
		ir.updateByPrepareReply(p)
		if p.OriginalBallot.IsInitialBallot() && !p.IsFromLeader {
			ir.identicalCount = 1
		}
		return
	}

	if ir.ballot.Compare(p.OriginalBallot) > 0 {
		return
	}

	if ir.ballot.Compare(p.OriginalBallot) < 0 {
		// Obviously, p.Ballot is not initial ballot,
		// in this case, we won't send accept next.
		ir.updateByPrepareReply(p)
		return
	}

	// original leader could go on fast path if
	// * initial ballot
	// * not from leader
	// * identical deps
	if p.OriginalBallot.IsInitialBallot() && !p.IsFromLeader &&
		ir.deps.SameAs(p.Deps) {
		ir.identicalCount++
	}
}

// This will load the cmds, deps, ballot, status
// from recovery info to the instance fields.
func (i *Instance) loadRecoveryInfo() {
	ir := i.recoveryInfo
	i.cmds, i.deps = ir.cmds.Clone(), ir.deps.Clone()

	if ir.ballot.Compare(i.ballot) > 0 || ir.statusIs(committed) {
		// to make sure we don't decrease the ballot except
		// for committed instance, because we want to have
		// same ballots for all committed instances.
		i.ballot = ir.ballot.Clone()
	}
}

// This function is the end of recovery phase and the instance have
// enough knowledge to broadcast messages to peers of his decision.
//
// One important aspect in EPaxos recovery phase is that it needs to
// take care of what's left on previous fast path made by default leader.
// Here are the details not covered in the paper:
//
// If the instance had received at least F identical preaccepted replies
// from non-leader replicas, we couldn't tell whether the default leader had chosen
// fast path or not. At this point, the instance should go to PAXOS-ACCEPT phase.
// And it is guaranteed safety:
// 1. if default leader hadn't gone to fast path, because (1) the instance had
//    prepared a quorum of peers, the final commit message wouldn't take outdated,
//    previous ones; and (2) including the default leader, there is
//    majority who knows the command, this guarantee that any conflict that happen
//    afterwards would know it. Therefore, this guarantees both consistency and
//    correctness of conflicting.
// 2. if the default leader had gone to fast path, then it must have sent
//    commit message of the same commands and dependencies as of the accept message
//    this instance sent for recovery. And this is also proven safe.
func (i *Instance) makeRecoveryDecision() (action uint8, msg message.Message) {
	i.loadRecoveryInfo()
	action = broadcastAction

	ir := i.recoveryInfo
	// determine status
	switch ir.status {
	case committed:
		i.enterCommitted()
		msg = i.makeCommit()
	case accepted:
		i.enterAcceptedAsSender()
		msg = i.makeAccept()
	case preAccepted:
		// If we have F identical replies from non-default-leader preaccepted
		// replias, we must send ACCEPT message.
		if ir.identicalCount >= i.replica.F() {
			i.enterAcceptedAsSender()
			msg = i.makeAccept()
		} else {
			i.enterPreAcceptedAsSender()
			msg = i.makePreAccept()
		}
	case nilStatus:
		// get ready to send pre-accept for No-op
		i.enterPreAcceptedAsSender()
		msg = i.makePreAccept()
	default:
		panic(ir.status)
	}
	return
}

// ****************************
// ******* Make Message *******
// ****************************

func (i *Instance) makePreAcceptReply(ok bool, deps message.Dependencies) *message.PreAcceptReply {
	return &message.PreAcceptReply{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Deps:       deps.Clone(),
		Ballot:     i.ballot.Clone(),
		From:       i.replica.Id,
	}
}

func (i *Instance) makePreAccept() *message.PreAccept {
	return &message.PreAccept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       i.cmds.Clone(),
		Deps:       i.deps.Clone(),
		Ballot:     i.ballot.Clone(),
		From:       i.replica.Id,
	}
}

func (i *Instance) makeAccept() *message.Accept {
	return &message.Accept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       i.cmds.Clone(),
		Deps:       i.deps.Clone(),
		Ballot:     i.ballot.Clone(),
		From:       i.replica.Id,
	}
}

func (i *Instance) makeAcceptReply(ok bool) *message.AcceptReply {
	return &message.AcceptReply{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     i.ballot.Clone(),
		From:       i.replica.Id,
	}
}

func (i *Instance) makeCommit() *message.Commit {
	return &message.Commit{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       i.cmds.Clone(),
		Deps:       i.deps.Clone(),
		From:       i.replica.Id,
	}
}

func (i *Instance) makePrepare() *message.Prepare {
	return &message.Prepare{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     i.ballot.Clone(),
		From:       i.replica.Id,
	}
}

// *******************************
// ******* State Transition ******
// *******************************

func (i *Instance) enterPreAcceptedAsSender() {
	i.checkStatus(nilStatus, preparing)
	i.status = preAccepted
	i.info.reset()
}

func (i *Instance) enterPreAcceptedAsReceiver() {
	i.checkStatus(nilStatus, preparing, preAccepted)
	i.status = preAccepted
}

func (i *Instance) enterAcceptedAsSender() {
	i.checkStatus(nilStatus, preAccepted, preparing)
	i.status = accepted
	i.info.reset()
}

func (i *Instance) enterAcceptedAsReceiver() {
	i.checkStatus(nilStatus, preAccepted, preparing, accepted)
	i.status = accepted
}

func (i *Instance) enterCommitted() {
	i.checkStatus(nilStatus, preAccepted, preparing, accepted)
	i.status = committed
	close(i.CommittedNotify)
}

func (i *Instance) enterPreparing() {
	i.checkStatus(nilStatus, preAccepted, preparing, accepted)

	i.initRecoveryInfo()

	// differentiates two cases on entering preparing:
	// - (new born) never seen anything of this instance before.
	// - seen any message about this instance before (with ballot).
	if i.isNewBorn() {
		i.ballot = i.replica.makeInitialBallot()
	} else {
		i.ballot.SetReplicaId(i.replica.Id)
	}
	i.ballot.IncNumber()

	i.status = preparing
}

// checkStatus checks the status of the instance
// it panics if the instance's status is not as expected
func (i *Instance) checkStatus(statusList ...uint8) {
	ok := false
	for _, status := range statusList {
		if i.isAtStatus(status) {
			ok = true
			break
		}
	}
	if !ok {
		panic(i.StatusString())
	}
}

// *******************************
// ******* Timeout Related *******
// *******************************
func (i *Instance) touch() {
	i.lastTouched = time.Now()
}

func (i *Instance) inactiveDuaration() time.Duration {
	p := time.Now().Sub(i.lastTouched)
	return p
}

// check if this instance is timeout
func (i *Instance) isTimeout() bool {
	if i.isBeforeStatus(committed) &&
		i.inactiveDuaration() > i.replica.TimeoutInterval {
		return true
	}
	return false
}

func (i *Instance) isExecuted() bool {
	return i.executed
}

func (i *Instance) SetExecuted() {
	i.executed = true
	close(i.ExecutedNotify)
}

func (i *Instance) isSender() bool {
	if i.isAtStatus(nilStatus) {
		return false
	}
	if i.ballot.GetReplicaId() == i.replica.Id {
		return true
	}
	return false
}

func (i *Instance) StatusString() string {
	switch i.status {
	case nilStatus:
		return "NilStatus"
	case preAccepted:
		return "PreAccepted"
	case accepted:
		return "Accepted"
	case committed:
		return "Committed"
	case preparing:
		return "Preparing"
	default:
		panic("")
	}
}

func (i *Instance) Pack() *PackedInstance {
	return &PackedInstance{
		Cmds:     i.cmds.Clone(),
		Deps:     i.deps.Clone(),
		Status:   i.status,
		Ballot:   i.ballot.Clone(),
		RowId:    i.rowId,
		Id:       i.id,
		Executed: i.executed,
		PackedRecoveryInfo: &PackedRecoveryInfo{
			Ballot:       i.recoveryInfo.ballot.Clone(),
			Cmds:         i.recoveryInfo.cmds.Clone(),
			Deps:         i.recoveryInfo.deps.Clone(),
			Status:       i.recoveryInfo.status,
			FormerStatus: i.recoveryInfo.formerStatus,
		},
	}
}

func (i *Instance) Unpack(p *PackedInstance) {
	i.cmds = p.Cmds.Clone()
	i.deps = p.Deps.Clone()
	i.status = p.Status
	i.ballot = p.Ballot.Clone()
	i.rowId = p.RowId
	i.id = p.Id
	i.executed = p.Executed

	if i.isAtStatus(preparing) {
		ri := i.recoveryInfo
		pr := p.PackedRecoveryInfo

		ri.ballot = pr.Ballot.Clone()
		ri.cmds = pr.Cmds.Clone()
		ri.deps = pr.Deps.Clone()
		ri.status = pr.Status
		ri.formerStatus = pr.FormerStatus
	}
}
