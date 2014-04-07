package livetest

import (
	"fmt"
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
		res[i] = (data.Commands{
			data.Command("c"),
			data.Command(strconv.Itoa(i)),
		})
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

	// test log correctness
	for i := 1; i <= maxInstance; i++ {
		for _, r := range nodes {
			if r.IsCheckpoint(uint64(i)) {
				break
			}
			logIndex := i - 1 - i/int(r.CheckpointCycle)
			assert.NotNil(t, r.InstanceMatrix[0][i])
			assert.Equal(t, r.InstanceMatrix[0][i].Commands(), allCmds[logIndex])
			expectedDeps := data.Dependencies{uint64(i - 1), 0, 0}
			assert.Equal(t, r.InstanceMatrix[0][i].Dependencies(), expectedDeps)
		}
	}
}

// Test Scenario: Non-conflict commands, 3 proposers
// Expect: All replicas have same correct logs(cmds, deps) eventually
func Test3Replica3ProposerNoConflict(t *testing.T) {
	N := 3
	maxInstance := 1024 * 48 // why this?
	allCmdsGroup := make([][]data.Commands, N)
	nodes := livetestlibSetupCluster(N)

	// setup expected logs
	for i := range allCmdsGroup {
		allCmdsGroup[i] = make([]data.Commands, maxInstance)
	}

	for i := 0; i < maxInstance; i++ {
		for j := range nodes {
			index := i*N + j
			cmds := livetestlibExampleCommands(index)
			nodes[j].BatchPropose(cmds)

			// record the correct log
			allCmdsGroup[j][i] = cmds
		}
	}
	time.Sleep(1000 * time.Microsecond)

	// test log correctness
	for i := 1; i <= maxInstance; i++ {
		// test over all instances
		for _, r := range nodes {
			if r.IsCheckpoint(uint64(i)) {
				break
			}
			// test over all log spaces of one instance
			for j := 0; j < N; j++ {
				// test commands
				logIndex := i - 1 - i/int(r.CheckpointCycle)
				assert.NotNil(t, r.InstanceMatrix[j][i])
				assert.Equal(t, r.InstanceMatrix[j][i].Commands(), allCmdsGroup[j][logIndex])

				// test depencies,
				// TODO: only partitial dependency now
				assert.Equal(t, r.InstanceMatrix[j][i].Dependencies()[j], uint64(i-1))
			}
		}
	}
}

//func Test3ReplicaConflict(t *testing.T) {
//	cfCmds := livetestlibConflictedCommands(2)
//	nodes := livetestlibSetupCluster(3)
//
//	maxInstance := 1024 * 48
//
//	for i := 0; i < maxInstance; i++ {
//		nodes[0].BatchPropose(cfCmds[0])
//		nodes[2].BatchPropose(cfCmds[1])
//	}
//
//	time.Sleep(1000 * time.Millisecond)
//
//	// check
//}
