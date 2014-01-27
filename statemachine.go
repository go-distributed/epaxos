package epaxos

import (
	"github.com/go-epaxos/epaxos/data"
)

type StateMachine interface {
	// Execute a batch of commands
	// Return the results in the interface array.
	// If the state machine failed during execution, an error will return and epaxos will stop accordingly.
	Execute(c []data.Command) ([]interface{}, error)
	// Test if there exists any conflicts in two group of commands
	HaveConflicts(c1 []data.Command, c2 []data.Command) bool
}
