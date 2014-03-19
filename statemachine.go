package epaxos

import (
	"errors"

	"github.com/go-distributed/epaxos/data"
)

var (
	ErrStateMachineExecution = errors.New("Statemachin execution error")
)

type StateMachine interface {
	// Execute a batch of commands
	// Return the results in the interface array.
	// If the state machine failed during execution, an error will return and epaxos will stop accordingly.
	// The error should be one of the errors above
	Execute(c []data.Command) ([]interface{}, error)
	// Test if there exists any conflicts in two group of commands
	HaveConflicts(c1 []data.Command, c2 []data.Command) bool
}
