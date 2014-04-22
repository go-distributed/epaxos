package livetest

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-distributed/epaxos/message"
	"github.com/go-distributed/epaxos/replica"
	"github.com/go-distributed/epaxos/test"
	"github.com/go-distributed/epaxos/transporter"
	"github.com/stretchr/testify/assert"
)

var _ = fmt.Printf
var _ = assert.Equal

func livetestlibExampleCommands(i int) message.Commands {
	return message.Commands{
		message.Command(strconv.Itoa(i)),
	}
}

func livetestlibConflictedCommands(total int) (res []message.Commands) {
	res = make([]message.Commands, total)
	for i := range res {
		res[i] = message.Commands{
			message.Command("c"),
			message.Command(strconv.Itoa(i)),
		}
	}
	return
}

func livetestlibSetupCluster(clusterSize int) []*replica.Replica {
	nodes := make([]*replica.Replica, clusterSize)

	for i := range nodes {
		param := &replica.Param{
			ExecuteInterval: time.Second * 50, // disable execution
			TimeoutInterval: time.Second * 50, // disable timeout
			ReplicaId:       uint8(i),
			Size:            uint8(clusterSize),
			StateMachine:    new(test.DummySM),
			Transporter:     transporter.NewDummyTR(uint8(i), clusterSize),
		}
		nodes[i], _ = replica.New(param)
	}

	chs := make([]chan message.Message, clusterSize)
	for i := range nodes {
		chs[i] = nodes[i].MessageChan
	}

	for i := range nodes {
		nodes[i].Transporter.(*transporter.DummyTransporter).RegisterChannels(chs)
		nodes[i].Start()
	}

	return nodes
}

func livetestlibStopCluster(nodes []*replica.Replica) {
	for _, r := range nodes {
		r.Stop()
	}
}

// This function tests the equality of two replicas'log
// for Instance[row]
func livetestlibLogCmpForTwo(t *testing.T, a, b *replica.Replica, row int) bool {
	if a.Size != b.Size {
		t.Fatal("Replica size not equal, this shouldn't happen")
	}

	end := b.MaxInstanceNum[row]
	if a.MaxInstanceNum[row] > b.MaxInstanceNum[row] {
		end = a.MaxInstanceNum[row]
	}

	for i := 0; i < int(end); i++ {
		if a.IsCheckpoint(uint64(i)) {
			continue
		}

		if a.InstanceMatrix[row][i] == nil {
			t.Logf("WARNING: Instance doesn't exist for replica[%d]:Instance[%d][%d]",
				a.Id, row, i)
		}

		if b.InstanceMatrix[row][i] == nil {
			t.Logf("WARNING: Instance doesn't exist for replica[%d]:Instance[%d][%d]",
				b.Id, row, i)
		}

		if a.InstanceMatrix[row][i].StatusString() != "Committed" {
			t.Logf("WARNING: Instance is not committed for replica[%d]:Instance[%d][%d]",
				a.Id, row, i)
		}

		if b.InstanceMatrix[row][i].StatusString() != "Committed" {
			t.Logf("WARNING: Instance is not committed for replica[%d]:Instance[%d][%d]",
				b.Id, row, i)
		}

		ca := a.InstanceMatrix[row][i].Commands()
		cb := b.InstanceMatrix[row][i].Commands()
		if !reflect.DeepEqual(ca, cb) {
			t.Logf("Cmds are not equal for replica[%d]:Instance[%d][%d] and replica[%d]:Instance[%d][%d]\n",
				a.Id, row, i, b.Id, row, i)
			t.Logf("%v, %v\n", ca, cb)
			return false
		}

		da := a.InstanceMatrix[row][i].Dependencies()
		db := b.InstanceMatrix[row][i].Dependencies()
		if !reflect.DeepEqual(da, db) {
			t.Logf("Deps are not equal for replica[%d]:Instance[%d][%d] and replica[%d]:Instance[%d][%d]\n",
				a.Id, row, i, b.Id, row, i)
			t.Logf("%v, %v\n", da, db)
			return false
		}
	}
	return true
}

// This is a Log comparation helper function, call this to check the log consistency.
// This func will return true if all logs(commands and dependencies) among a group of replicas
// are identical.
func livetestlibLogConsistent(t *testing.T, replicas ...*replica.Replica) bool {
	size := int(replicas[0].Size)
	for i := range replicas {
		next := (i + 1) % size
		for j := 0; j < size; j++ {
			if !livetestlibLogCmpForTwo(t, replicas[i], replicas[next], j) {
				return false
			}
		}
	}
	return true
}

// This func tests if the log has correctly record the conflicts
// return true if dep[row] == instanceId or dep[row] is a checkpoint
// r is just for check if instanceId is a checkpoint
func liveTestlibHaveConflicts(r *replica.Replica, deps message.Dependencies, row int, instanceId uint64) bool {
	if deps[row] == instanceId || r.IsCheckpoint(deps[row]) {
		return true
	}
	return false
}

////////////////////////////////////////////////////////
//                                                    //
//                      Tests                         //
//                                                    //
////////////////////////////////////////////////////////

func liveTestlibVerifyDependency(r *replica.Replica, pos uint64) bool {
	N := len(r.InstanceMatrix)

	deps := make([]message.Dependencies, N)
	for i, inst := range r.InstanceMatrix {
		if inst[pos] == nil {
			return true
		}
		deps[i] = inst[pos].Dependencies()
	}

	// check if the one depend on the other, or if it's a checkpoint
	for p := 0; p < N; p++ {
		for q := 0; q < N; q++ {
			if p == 0 {
				continue
			}
			if liveTestlibHaveConflicts(r, deps[p], q, pos) {
				return true
			}
		}
	}
	return false
}

// Test Scenario: Non-conflict commands, 1 proposer
// Expect: All replicas have same correct logs(cmds, deps) eventually
func Test3Replica1ProposerNoConflict(t *testing.T) {
	maxInstance := 1024 * 4
	allCmds := make([]message.Commands, maxInstance)

	nodes := livetestlibSetupCluster(3)
	defer livetestlibStopCluster(nodes)

	for i := 0; i < maxInstance; i++ {
		cmds := livetestlibExampleCommands(i)
		go nodes[0].Propose(cmds...) // batching disabled
		allCmds[i] = cmds
	}
	fmt.Println("Wait 5 seconds for completion")
	time.Sleep(5 * time.Second)

	// test log consistency
	assert.True(t, livetestlibLogConsistent(t, nodes...))
}

// Test Scenario: Non-conflict commands, 3 proposers
// Expect: All replicas have same correct logs(cmds, deps) eventually
func Test3Replica3ProposerNoConflict(t *testing.T) {
	N := 3
	maxInstance := 1024 * 4
	nodes := livetestlibSetupCluster(N)
	defer livetestlibStopCluster(nodes)

	for i := 0; i < maxInstance; i++ {
		for j := range nodes {
			index := i*N + j
			cmds := livetestlibExampleCommands(index)
			go nodes[j].Propose(cmds...) // batching disabled
		}
	}
	fmt.Println("Wait 5 Seconds for completion")
	time.Sleep(5 * time.Second)

	assert.True(t, livetestlibLogConsistent(t, nodes...))
}

func Test2ProposerConflict(t *testing.T) {
	maxInstance := 2048
	nodes := livetestlibSetupCluster(3)
	defer livetestlibStopCluster(nodes)

	// node 0 and 1 are conflicted with each other
	for i := 1; i < maxInstance; i++ {
		for j := 0; j < 2; j++ {
			cmds := livetestlibExampleCommands(i)
			go nodes[j].Propose(cmds...) // batching disabled
		}
	}
	fmt.Println("Wait 5 Seconds for completion")
	time.Sleep(5 * time.Second)

	assert.True(t, livetestlibLogConsistent(t, nodes...))

	for i := 1; i < maxInstance; i++ {
		if nodes[0].IsCheckpoint(uint64(i)) {
			continue
		}

		pos := uint64(i)
		assert.True(t, liveTestlibVerifyDependency(nodes[0], pos))
	}
}

func Test3ProposerConflict(t *testing.T) {
	N := 3
	maxInstance := 2048
	nodes := livetestlibSetupCluster(N)
	defer livetestlibStopCluster(nodes)

	// node 0 must conflict with 1, maybe with 2
	// node 1 must conflict with 0, maybe with 2
	// node 2 must conflict with 0, maybe with 1
	for i := 1; i < maxInstance; i++ {
		for j := 0; j < N; j++ {
			cmds := livetestlibExampleCommands(i)
			go nodes[j].Propose(cmds...) //batching disabled
		}
	}
	fmt.Println("Wait 5 seconds for completion")
	time.Sleep(5 * time.Second)

	assert.True(t, livetestlibLogConsistent(t, nodes...))

	for i := 1; i < maxInstance; i++ {
		if nodes[0].IsCheckpoint(uint64(i)) {
			continue
		}

		pos := uint64(i)
		assert.True(t, liveTestlibVerifyDependency(nodes[0], pos))
	}
}
