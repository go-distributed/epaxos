package message

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var _ = fmt.Printf

func TestCommandCompareAndCopy(t *testing.T) {
	self := Commands{
		Command("hello"),
		Command("world"),
	}
	assert.True(t, self[0].Compare(self[1]) != 0)

	other := self.Clone()

	assert.Equal(t, self, other)
	assert.True(t, &self != &other)

	for i := range self {
		assert.True(t, &self[i] != &other[i])
	}
}

func TestCommandClone(t *testing.T) {
	nilCmds := Commands(nil)
	assert.Equal(t, nilCmds.Clone(), Commands(nil))
	manyCmds := Commands{
		Command("1"),
		Command("2"),
	}
	assert.True(t,
		assert.ObjectsAreEqual(manyCmds.Clone(), manyCmds),
	)
}
