package epaxos

import (
	"errors"

	"github.com/go-distributed/epaxos/message"
)

var (
	ErrStateMachineExecution = errors.New("Statemachin execution error")
)

type StateMachine interface {
	// Execute a batch of commands
	// Return the results in the interface array.
	// If the state machine failed during execution, an error will return and epaxos will stop accordingly.
	// The error should be one of the errors above
	Execute(c []message.Command) ([]interface{}, error)
	// Test if there exists any conflicts in two group of commands
	HaveConflicts(c1 []message.Command, c2 []message.Command) bool
}
