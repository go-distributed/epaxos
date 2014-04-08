package replica

// This file implements instance module.
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

import (
	"fmt"

	"github.com/go-distributed/epaxos/data"
)

var _ = fmt.Printf

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
	cmds   data.Commands
	deps   data.Dependencies
	status uint8
	ballot *data.Ballot

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
	ballot     *data.Ballot // for book keeping

	cmds         data.Commands
	deps         data.Dependencies
	status       uint8
	formerStatus uint8
	formerBallot *data.Ballot
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
		ballot:       data.NewBallot(0, 0, 0),
		status:       nilStatus,
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
		ballot:       data.NewBallot(0, 0, 0),
		formerBallot: data.NewBallot(0, 0, 0),
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

func (i *Instance) isAtInitialRound() bool {
	return i.ballot.Epoch() == 0
}

func (i *Instance) Commands() data.Commands {
	return i.cmds
}

func (i *Instance) Dependencies() data.Dependencies {
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
	ir.status = i.status
	ir.formerStatus = i.status
	ir.formerBallot = i.ballot.Clone()
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

func (r *RecoveryInfo) updateByPrepareReply(p *data.PrepareReply) {
	r.cmds, r.deps = p.Cmds, p.Deps
	r.ballot, r.status = p.OriginalBallot, p.Status
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
func (i *Instance) nilStatusProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(nilStatus, preAccepted, accepted, committed, preparing)

	if !i.isAtStatus(nilStatus) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.Propose:
		return i.handlePropose(content)
	case *data.PreAccept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPreAccept()
		}
		return i.handlePreAccept(content)
	case *data.Accept:
		if content.Ballot.Compare(i.ballot) < 0 {
			// [*] this could happens when the instance revert from preparing
			return i.rejectAccept()
		}
		return i.handleAccept(content)
	case *data.Commit:
		return i.handleCommit(content)
	case *data.Prepare:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPrepare()
		}
		return i.handlePrepare(content)
	case *data.PrepareReply:
		if i.isAtInitialRound() {
			panic("Never send prepare before but receive prepare reply")
		}
		return noAction, nil
	case *data.PreAcceptReply, *data.AcceptReply, *data.PreAcceptOk:
		panic("")
	default:
		panic("")
	}
}

// preaccepted instance
// - handles preaccept-ok/-reply, preaccept, accept, commit, and prepare.
func (i *Instance) preAcceptedProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(preAccepted, accepted, committed)

	if !i.isAtStatus(preAccepted) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAccept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPreAccept()
		}
		return i.handlePreAccept(content)
	case *data.Accept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectAccept()
		}
		return i.handleAccept(content)
	case *data.Commit:
		return i.handleCommit(content)
	case *data.Prepare:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPrepare()
		}
		return i.handlePrepare(content)
	case *data.PreAcceptReply:
		if content.Ballot.Compare(i.ballot) < 0 {
			// ignore stale PreAcceptReply
			return noAction, nil
		}
		return i.handlePreAcceptReply(content)
	case *data.PreAcceptOk:
		if !i.ballot.IsInitialBallot() {
			return noAction, nil // ignore stale reply
		}
		return i.handlePreAcceptOk(content)
	case *data.AcceptReply:
		panic("")
	case *data.PrepareReply:
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
func (i *Instance) acceptedProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(accepted, committed)

	if !i.isAtStatus(accepted) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAccept:
		return i.rejectPreAccept()
	case *data.Accept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectAccept()
		}
		return i.handleAccept(content)
	case *data.Commit:
		return i.handleCommit(content)
	case *data.Prepare:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPrepare()
		}
		return i.handlePrepare(content)
	case *data.AcceptReply:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil // ignore stale PreAcceptReply
		}
		return i.handleAcceptReply(content)
	case *data.PreAcceptReply, *data.PreAcceptOk:
		return noAction, nil // ignore stale replies
	case *data.PrepareReply:
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
func (i *Instance) committedProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(committed)

	if !i.isAtStatus(committed) {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAccept:
		return i.rejectPreAccept()
	case *data.Accept:
		return i.rejectAccept()
	case *data.Prepare:
		return i.handlePrepare(content)
	case *data.PreAcceptReply, *data.PreAcceptOk, *data.AcceptReply, *data.PrepareReply, *data.Commit:
		return noAction, nil // ignore stale replies
	default:
		panic("")
	}
}

// preparing instance could only acts as a sender.
// It handles most kinds of messages (in some conditions with larger ballot) and
// ignores all replies except prepare reply.
func (i *Instance) preparingProcess(m Message) (action uint8, msg Message) {
	defer i.checkStatus(preparing, preAccepted, accepted, committed, nilStatus)

	if !i.isAtStatus(preparing) || i.recoveryInfo == nil {
		panic("")
	}

	switch content := m.Content().(type) {
	case *data.PreAccept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPreAccept()
		}
		return i.handlePreAccept(content)
	case *data.Accept:
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectAccept()
		}
		return i.handleAccept(content)
	case *data.Commit:
		return i.handleCommit(content)
	case *data.Prepare:
		// the instance itself is the first one to have ballot of this
		// magnitude. It can't receive others having the same
		if content.Ballot.Compare(i.ballot) == 0 {
			panic("")
		}
		if content.Ballot.Compare(i.ballot) < 0 {
			return i.rejectPrepare()
		}
		return i.revertAndHandlePrepare(content)
	case *data.PrepareReply:
		if content.Ballot.Compare(i.ballot) < 0 {
			return noAction, nil
		}
		return i.handlePrepareReply(content)
	case *data.PreAcceptReply:
		if i.recoveryInfo.formerStatus < preAccepted {
			panic("")
		}
		return noAction, nil
	case *data.PreAcceptOk:
		if i.recoveryInfo.formerStatus < preAccepted {
			panic("")
		}
		return noAction, nil
	case *data.AcceptReply:
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
// ****** Reject Messages *******
// ******************************

// -------- REJECT CONDITIONS --------
// When someone rejected a message (pre-accept, accept, prepare), it implied that
// the rejector had larger ballot than the message sent. In such a case, we just need
// to return {ok: false, rejector's ballot, and relevant ids}
// -----------------------------------

// PreAccept reply:
// - ok : false
// - Ballot: self (ballot)
// - Ids
func (i *Instance) rejectPreAccept() (action uint8, reply *data.PreAcceptReply) {
	return replyAction, i.makePreAcceptReply(false, nil)
}

// rejectAccept rejects the Accept request with a AcceptReply:
// - Ok: false
// - Ballot: self.ballot
// - ReplicaId: self.rowId
// - InstanceId: self.id
// - other fields: undefined
func (i *Instance) rejectAccept() (action uint8, reply *data.AcceptReply) {
	return replyAction, i.makeAcceptReply(false)
}

// Prepare reply:
// - ok : false
// - Ballot: self (ballot)
// - relevant Ids
func (i *Instance) rejectPrepare() (action uint8, reply *data.PrepareReply) {
	return replyAction, &data.PrepareReply{
		Ok:         false,
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     i.ballot.Clone(),
	}
}

// ******************************
// ****** Handle Message  *******
// ******************************

// a propose will broadcasted to fast quorum in pre-accept message.
func (i *Instance) handlePropose(p *data.Propose) (action uint8, msg *data.PreAccept) {
	if p.Cmds == nil || !i.isAtInitialRound() || !i.isAtStatus(nilStatus) {
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
func (i *Instance) handlePreAccept(p *data.PreAccept) (action uint8, msg Message) {
	if p.Ballot.Compare(i.ballot) < 0 {
		panic("")
	}

	i.enterPreAcceptedAsReceiver()
	i.ballot = p.Ballot
	changed := i.replica.updateInstance(p.Cmds, p.Deps, i.rowId, i)

	if changed {
		return replyAction, i.makePreAcceptReply(true, i.deps)
	}

	// not initial leader
	if !p.Ballot.IsInitialBallot() {
		return replyAction, i.makePreAcceptReply(true, i.deps)
	}
	// pre-accept-ok for possible fast quorum commit
	return replyAction, &data.PreAcceptOk{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
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
func (i *Instance) commonPreAcceptedNextStep() (action uint8, msg Message) {
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
func (i *Instance) handlePreAcceptOk(p *data.PreAcceptOk) (action uint8, msg Message) {
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
func (i *Instance) handlePreAcceptReply(p *data.PreAcceptReply) (action uint8, msg Message) {
	if p.Ballot.Compare(i.ballot) < 0 {
		panic("")
	}
	if p.Ballot.Compare(i.ballot) > 0 {
		if p.Ok {
			panic("")
		}
		i.ballot = p.Ballot
		return noAction, nil
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
func (i *Instance) handleAccept(a *data.Accept) (action uint8, msg *data.AcceptReply) {
	if a.Ballot.Compare(i.ballot) < 0 {
		panic("")
	}
	if i.isAfterStatus(accepted) ||
		i.isAtStatus(accepted) && a.Ballot.Compare(i.ballot) == 0 {
		panic("")
	}

	i.cmds, i.deps, i.ballot = a.Cmds, a.Deps, a.Ballot
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
func (i *Instance) handleAcceptReply(a *data.AcceptReply) (action uint8, msg Message) {
	if a.Ballot.Compare(i.ballot) < 0 {
		panic("")
	}

	// negative reply
	if a.Ballot.Compare(i.ballot) > 0 {
		// [*] there may be stale but large ballots,
		// if we receive such ballots, that means there may be another newer proposer,
		// so we'd better step down by increasing our own ballot so we can ignore
		// the following replies.
		if a.Ok {
			panic("")
		}
		i.ballot = a.Ballot
		return noAction, nil
	}

	if !a.Ok {
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
func (i *Instance) handleCommit(c *data.Commit) (action uint8, msg Message) {
	if i.isAtOrAfterStatus(committed) {
		panic("")
	}

	i.cmds, i.deps = c.Cmds, c.Deps
	i.enterCommitted()

	// TODO: Do we need to clear unnecessary objects to save more memory?
	// TODO: persistent
	return noAction, nil
}

func (i *Instance) revertAndHandlePrepare(p *data.Prepare) (action uint8, msg *data.PrepareReply) {
	i.checkStatus(preparing)
	i.status = i.recoveryInfo.formerStatus
	i.ballot = i.recoveryInfo.formerBallot
	return i.handlePrepare(p)
}

func (i *Instance) handlePrepare(p *data.Prepare) (action uint8, msg *data.PrepareReply) {
	oldBallot := i.ballot.Clone()

	// if the instance is alreay a commited one,
	// then it just replies with ok == true,
	// and does not update its ballot, so only
	// non-committed instance will need to update self ballot.
	if !i.isAtStatus(committed) {
		if p.Ballot.Compare(i.ballot) <= 0 { // cannot be equal or smaller
			panic(fmt.Sprintln("prepare ballot: ", p.Ballot, i.ballot))
		}
		i.ballot = p.Ballot
	}

	isFromLeader := false
	if i.replica.Id == i.rowId {
		isFromLeader = true
	}

	return replyAction, &data.PrepareReply{
		Ok:             true,
		ReplicaId:      i.rowId,
		InstanceId:     i.id,
		Status:         i.status,
		Cmds:           i.cmds.Clone(),
		Deps:           i.deps.Clone(),
		Ballot:         p.Ballot.Clone(),
		OriginalBallot: oldBallot,
		IsFromLeader:   isFromLeader,
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
func (i *Instance) handlePrepareReply(p *data.PrepareReply) (action uint8, msg Message) {
	if !i.isAtStatus(preparing) {
		panic("")
	}

	if p.Ballot.Compare(i.ballot) < 0 {
		panic("")
	}
	// negative reply
	if p.Ballot.Compare(i.ballot) > 0 {
		if p.Ok {
			panic("")
		}
		i.ballot = p.Ballot
		return noAction, nil
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

func (i *Instance) updateRecoveryInstance(p *data.PrepareReply) {
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

func (i *Instance) handleCommittedPrepareReply(p *data.PrepareReply) {
	if i.recoveryInfo.statusIs(committed) {
		return
	}
	i.recoveryInfo.updateByPrepareReply(p)
}

func (i *Instance) handleAcceptedPrepareReply(p *data.PrepareReply) {
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

func (i *Instance) handlePreAcceptedPrepareReply(p *data.PrepareReply) {
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
	i.cmds, i.deps = ir.cmds, ir.deps
	i.ballot = ir.ballot
}

func (i *Instance) makeRecoveryDecision() (action uint8, msg Message) {
	i.loadRecoveryInfo()

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
		// if former leader committed on fast-path,
		// then we must send accept instead of pre-accept
		if ir.identicalCount >= i.replica.quorum() {
			i.enterAcceptedAsSender()
			msg = i.makeAccept()

		} else {
			i.enterPreAcceptedAsSender()
			msg = i.makePreAccept()
		}
	case nilStatus:
		// get ready to send Accept for No-op
		i.enterAcceptedAsSender()
		msg = i.makeAccept()
	default:
		panic("")
	}
	action = broadcastAction
	return
}

// ****************************
// ******* Make Message *******
// ****************************

func (i *Instance) makePreAcceptReply(ok bool, deps data.Dependencies) *data.PreAcceptReply {
	return &data.PreAcceptReply{
		Ok:         ok,
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Deps:       deps,
		Ballot:     i.ballot.Clone(),
	}
}

func (i *Instance) makePreAccept() *data.PreAccept {
	return &data.PreAccept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       i.cmds.Clone(),
		Deps:       i.deps.Clone(),
		Ballot:     i.ballot.Clone(),
	}
}

func (i *Instance) makeAccept() *data.Accept {
	return &data.Accept{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       i.cmds.Clone(),
		Deps:       i.deps.Clone(),
		Ballot:     i.ballot.Clone(),
	}
}

func (i *Instance) makeAcceptReply(ok bool) *data.AcceptReply {
	return &data.AcceptReply{
		Ok:         ok,
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Ballot:     i.ballot.Clone(),
	}
}

func (i *Instance) makeCommit() *data.Commit {
	return &data.Commit{
		ReplicaId:  i.rowId,
		InstanceId: i.id,
		Cmds:       i.cmds, // [*] no need to clone
		Deps:       i.deps, // [*] no need to clone either
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
}

func (i *Instance) enterPreparing() {
	i.checkStatus(nilStatus, preAccepted, preparing, accepted)

	i.initRecoveryInfo()

	// differentiates two cases on entering preparing:
	// - seen any message about this instance before (with ballot).
	// - never seen anything concerning this instance before.
	if i.isAtInitialRound() {
		// epoch.1.id
		i.ballot = i.replica.makeInitialBallot()
		i.ballot.IncNumber()
	} else {
		i.ballot.SetReplicaId(i.replica.Id)
		i.ballot.IncNumber()
	}

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
		panic("")
	}
}

func (i *Instance) isExecuted() bool {
	return i.executed
}

func (i *Instance) SetExecuted() {
	i.executed = true
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
	default:
		panic("")
	}
}
