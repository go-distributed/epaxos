package main

import (
	"github.com/go-distributed/epaxos/data"
)

type Voter struct {
}

func (v *Voter) Execute(c []data.Command) ([]interface{}, error) {
	panic("")
}
func (v *Voter) HaveConflicts(c1 []data.Command, c2 []data.Command) bool {
	return true
}
