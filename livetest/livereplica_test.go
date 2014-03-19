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

func livetestlibExampleCommands() data.Commands {
	return data.Commands{
		data.Command("hello"),
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

func Test3ReplicaReplication(t *testing.T) {
	cmds := livetestlibExampleCommands()
	nodes := livetestlibSetupCluster(3)

	maxInstance := 1024 * 48

	for i := 0; i < maxInstance; i++ {
		nodes[0].BatchPropose(cmds)
	}

	time.Sleep(1000 * time.Millisecond)

	for i := 1; i <= maxInstance; i++ {
		for _, r := range nodes {
			if r.IsCheckpoint(uint64(i)) {
				break
			}
			assert.NotNil(t, r.InstanceMatrix[0][i])
			assert.Equal(t, r.InstanceMatrix[0][i].Commands(), cmds)
		}
	}
}

func Test3ReplicaConflict(t *testing.T) {
	cfCmds := livetestlibConflictedCommands(2)
	nodes := livetestlibSetupCluster(3)

	maxInstance := 1024 * 48

	for i := 0; i < maxInstance; i++ {
		nodes[0].BatchPropose(cfCmds[0])
		nodes[2].BatchPropose(cfCmds[1])
	}

	time.Sleep(1000 * time.Millisecond)

	// check
}
