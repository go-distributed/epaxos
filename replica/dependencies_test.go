package replica

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnionNilPanic(t *testing.T) {
	defer func() {
		r := recover()
		assert.Equal(t, r, "union: dependencis should not be nil")
	}()

	self := new(dependencies)
	self.union(nil)
}

func TestUnionSizePanic(t *testing.T) {
	defer func() {
		r := recover()
		assert.Equal(t, r, "union: size different!")
	}()

	self := make(dependencies, 10)
	other := make(dependencies, 5)
	self.union(other)
}

// TestUnion tests the result of the union operation
func TestUnion(t *testing.T) {
	self := make(dependencies, 5)
	for i := range self {
		self[i] = uint64(i)
	}

	other := make(dependencies, 5)
	for i := range other {
		other[i] = uint64(i)
	}
	same := self.union(other)
	assert.True(t, same)

	other = make(dependencies, 5)
	for i := range other {
		other[i] = uint64(5 - i)
	}
	same = self.union(other)

	assert.False(t, same)
	assert.Equal(t, self, dependencies{5, 4, 3, 3, 4})
}
