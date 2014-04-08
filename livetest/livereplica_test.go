package livetest

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-distributed/epaxos/data"
	"github.com/go-distributed/epaxos/replica"
	"github.com/go-distributed/epaxos/test"

	"github.com/stretchr/testify/assert"
)

var _ = fmt.Printf
var _ = assert.Equal

func livetestlibExampleCommands(i int) data.Commands {
	return data.Commands{
		data.Command(strconv.Itoa(i)),
	}
}

func livetestlibConflictedCommands(total int) (res []data.Commands) {
	res = make([]data.Commands, total)
	for i := 0; i < total; i++ {
		res[i] = data.Commands{
			data.Command("c"),
			data.Command(strconv.Itoa(i)),
		}
	}
	return
}

func livetestlibSetupCluster(clusterSize int) []*replica.Replica {
	nodes := make([]*replica.Replica, clusterSize)

	param := &replica.Param{
		Size:         uint8(clusterSize),
		StateMachine: new(test.DummySM),
	}
	for i := 0; i < clusterSize; i++ {
		param.ReplicaId = uint8(i)
		nodes[i] = replica.New(param)
	}

	for i := 0; i < clusterSize; i++ {
		nodes[i].Transporter = &DummyTransporter{
			Nodes:      nodes,
			Self:       uint8(i),
			FastQuorum: uint8(clusterSize) - 2,
			All:        uint8(clusterSize),
		}
		nodes[i].Start()
	}

	return nodes
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
		if !reflect.DeepEqual(
			a.InstanceMatrix[row][i].Commands(),
			b.InstanceMatrix[row][i].Commands()) {
			t.Logf("Cmds are not equal for replica[%d]:Instance[%d][%d] and replica[%d]:Instance[%d][%d]\n",
				a.Id, row, i, b.Id, row, i)
			return false
		}
		if !reflect.DeepEqual(
			a.InstanceMatrix[row][i].Dependencies(),
			b.InstanceMatrix[row][i].Dependencies()) {
			t.Logf("Deps are not equal for replica[%d]:Instance[%d][%d] and replica[%d]:Instance[%d][%d]\n",
				a.Id, row, i, b.Id, row, i)
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
	maxInstance := 1024 * 48
	allCmds := make([]data.Commands, maxInstance)

	nodes := livetestlibSetupCluster(3)

	for i := 0; i < maxInstance; i++ {
		cmds := livetestlibExampleCommands(i)
		nodes[0].BatchPropose(cmds)
		allCmds[i] = cmds
	}
	time.Sleep(1000 * time.Millisecond)

	// test log consistency
	assert.True(t, livetestlibLogConsistent(t, nodes...))
}

// Test Scenario: Non-conflict commands, 3 proposers
// Expect: All replicas have same correct logs(cmds, deps) eventually
func Test3Replica3ProposerNoConflict(t *testing.T) {
	N := 3
	maxInstance := 1024 * 4
	nodes := livetestlibSetupCluster(N)

	for i := 0; i < maxInstance; i++ {
		for j := range nodes {
			index := i*N + j
			cmds := livetestlibExampleCommands(index)
			nodes[j].BatchPropose(cmds)
		}
	}
	time.Sleep(100 * time.Microsecond)

	assert.True(t, livetestlibLogConsistent(t, nodes...))
}

func Test2ProposerConflict(t *testing.T) {
	maxInstance := 1024
	nodes := livetestlibSetupCluster(3)

	for i := 1; i < maxInstance; i++ {
		for j := 0; j < 2; j++ {
			cmds := livetestlibExampleCommands(i)
			nodes[j].BatchPropose(cmds)
		}
	}

	time.Sleep(100 * time.Microsecond)

	assert.True(t, livetestlibLogConsistent(t, nodes...))

	for i := 1; i < maxInstance; i++ {
		deps1 := nodes[0].InstanceMatrix[0][i].Dependencies()
		deps2 := nodes[0].InstanceMatrix[1][i].Dependencies()
		pos := uint64(i)
		if deps1[1] == pos && deps2[0] == pos {
			continue
		}
		t.Fatal("Incorrect conflict")
	}
}
