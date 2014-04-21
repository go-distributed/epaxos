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
			ReplicaId:    uint8(i),
			Size:         uint8(clusterSize),
			StateMachine: new(test.DummySM),
			Transporter:  transporter.NewDummyTR(uint8(i), clusterSize),
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

// Test Scenario: Non-conflict commands, 1 proposer
// Expect: All replicas have same correct logs(cmds, deps) eventually
func Test3Replica1ProposerNoConflict(t *testing.T) {
	maxInstance := 1024 * 4
	allCmds := make([]message.Commands, maxInstance)

	nodes := livetestlibSetupCluster(3)
	defer livetestlibStopCluster(nodes)

	for i := 0; i < maxInstance; i++ {
		cmds := livetestlibExampleCommands(i)
		nodes[0].Propose(cmds...) // batching disabled
		allCmds[i] = cmds
	}
	fmt.Println("Wait 5000 millis for completion")
	time.Sleep(5000 * time.Millisecond)

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
			nodes[j].Propose(cmds...) // batching disabled
		}
	}
	fmt.Println("Wait 5000 millis for completion")
	time.Sleep(5000 * time.Millisecond)

	assert.True(t, livetestlibLogConsistent(t, nodes...))
}

func Test2ProposerConflict(t *testing.T) {
	maxInstance := 1024
	nodes := livetestlibSetupCluster(3)
	defer livetestlibStopCluster(nodes)

	// node 0 and 1 are conflicted with each other
	for i := 1; i < maxInstance; i++ {
		for j := 0; j < 2; j++ {
			cmds := livetestlibExampleCommands(i)
			nodes[j].Propose(cmds...) // batching disabled
		}
	}
	fmt.Println("Wait 5000 millis for completion")
	time.Sleep(5000 * time.Millisecond)

	assert.True(t, livetestlibLogConsistent(t, nodes...))

	for i := 1; i < maxInstance; i++ {
		deps1 := nodes[0].InstanceMatrix[0][i].Dependencies()
		deps2 := nodes[0].InstanceMatrix[1][i].Dependencies()
		pos := uint64(i)

		if !reflect.DeepEqual(deps1[1], pos) {
			t.Fatal("Incorrect dependencies", i, deps1)
		}

		if !reflect.DeepEqual(deps2[0], pos) {
			t.Fatal("Incorrect dependencies", i, deps2)
		}
	}
}

func Test3ProposerConflict(t *testing.T) {
	maxInstance := 1024
	nodes := livetestlibSetupCluster(3)
	defer livetestlibStopCluster(nodes)

	// node 0 must conflict with 1, maybe with 2
	// node 1 must conflict with 0, maybe with 2
	// node 2 must conflict with 0, maybe with 1
	for i := 1; i < maxInstance; i++ {
		for j := 0; j < 3; j++ {
			cmds := livetestlibExampleCommands(i)
			nodes[j].Propose(cmds...) //batching disabled
		}
	}
	fmt.Println("Wait 5000 millis for completion")
	time.Sleep(5000 * time.Millisecond)

	assert.True(t, livetestlibLogConsistent(t, nodes...))

	deps := make([]message.Dependencies, 3)
	for i := 1; i < maxInstance; i++ {
		deps[0] = nodes[0].InstanceMatrix[0][i].Dependencies()
		deps[1] = nodes[0].InstanceMatrix[1][i].Dependencies()
		deps[2] = nodes[0].InstanceMatrix[2][i].Dependencies()
		pos := uint64(i)

		if !reflect.DeepEqual(deps[0][1], pos) {
			t.Fatal("Incorrect dependencies", i, deps[0])
		}

		if !reflect.DeepEqual(deps[1][0], pos) {
			t.Fatal("Incorrect dependencies", i, deps[1])
		}

		if !reflect.DeepEqual(deps[2][0], pos) {
			t.Fatal("Incorrect dependencies", i, deps[2])
		}
	}
}
