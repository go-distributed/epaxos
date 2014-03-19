package test

import (
	"bytes"

	"github.com/go-distributed/epaxos"
	"github.com/go-distributed/epaxos/data"
)

type DummySM struct {
	ExecutionLog []string
}

func NewDummySM() *DummySM {
	return &DummySM{
		ExecutionLog: make([]string, 0),
	}
}

func (d *DummySM) Execute(c []data.Command) ([]interface{}, error) {
	result := make([]interface{}, 0)
	for i := range c {
		if bytes.Compare(c[i], data.Command("error")) == 0 {
			return nil, epaxos.ErrStateMachineExecution
		}
		result = append(result, string(c[i]))
		d.ExecutionLog = append(d.ExecutionLog, string(c[i]))
	}
	return result, nil
}

func (d *DummySM) HaveConflicts(c1 []data.Command, c2 []data.Command) bool {
	for i := range c1 {
		for j := range c2 {
			if bytes.Compare(c1[i], c2[j]) == 0 {
				return true
			}
		}
	}
	return false
}
