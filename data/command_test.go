package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandCompareAndCopy(t *testing.T) {
	self := Commands{
		Command("hello"),
		Command("world"),
	}
	assert.True(t, self[0].Compare(self[1]) != 0)

	other := self.GetCopy()

	assert.True(t, &self != &other)
	assert.True(t, self[0].Compare(other[0]) == 0)

	assert.Equal(t, self, other)
}
