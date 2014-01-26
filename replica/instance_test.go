package replica

import (
	"testing"

	"github.com/go-epaxos/epaxos/data"
	"github.com/go-epaxos/epaxos/test"
        "github.com/stretchr/testify/assert"
)

// When a committed instance receives pre-accept message, it should ignore it
func TestCommittedProcessPreAccept(t *testing.T) {
	// create an new instance
	r := New(0, 5, new(test.DummySM))
        i := NewInstance(r, conflictNotFound + 1)
	// set its status to committed
	i.status = committed
	// send a pre-accept message to it
        msg := &data.PreAccept{

        }
        action, retMsg := i.committedProcess(msg)

	// expect:
	// - action: NoAction
	// - message: nil
	// - instance not changed
        assert.Equal(t, action, noAction, "")
        assert.Nil(t, retMsg, "")
}

func InstanceTestExamplePreAccept() {
}
