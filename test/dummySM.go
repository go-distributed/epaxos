package test

import (
	"bytes"
	"errors"

	"github.com/go-distributed/epaxos/data"
)

type DummySM bool

func (d *DummySM) Execute(c []data.Command) ([]interface{}, error) {
	result := make([]interface{}, 0)
	for i := range c {
		if bytes.Compare(c[i], data.Command("error")) == 0 {
			return nil, errors.New("error")
		}
		result = append(result, string(c[i]))
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
