package livetest

import (
	"fmt"
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

func livetestlibSetupCluster(clusterSize int) []*replica.Replica {
	nodes := make([]*replica.Replica, clusterSize)

	for i := 0; i < clusterSize; i++ {
		nodes[i] = replica.New(uint8(i), uint8(clusterSize), new(test.DummySM))
	}

	for i := 0; i < clusterSize; i++ {
		nodes[i].Transporter = &DummyTransporter{
			Nodes:      nodes,
			Self:       uint8(i),
			FastQuorum: uint8(clusterSize) - 2,
			All:        uint8(clusterSize),
		}
		nodes[i].GoStart()
	}

	return nodes
}

func Test3ReplicaReplication(t *testing.T) {
	cmds := livetestlibExampleCommands()
	nodes := livetestlibSetupCluster(3)

        maxInstance := 1024*48

	for i := 0; i < maxInstance; i++ {
		nodes[0].Propose(cmds)
	}

	time.Sleep(1000 * time.Millisecond)

	for i := 1; i <= maxInstance; i++ {
		for _, r := range nodes {
			assert.NotNil(t, r.InstanceMatrix[0][i])
                        assert.Equal(t, r.InstanceMatrix[0][i].Commands(), cmds)
		}
	}

}


