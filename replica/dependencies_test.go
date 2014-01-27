package replica

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnionNilPanic(t *testing.T) {
	defer func() {
		r := recover()
		assert.Equal(t, r, "Union: dependencis should not be nil")
	}()

	self := new(dependencies)
	self.Union(nil)
}

func TestUnionSizePanic(t *testing.T) {
	defer func() {
		r := recover()
		assert.Equal(t, r, "Union: size different!")
	}()

	self := make(dependencies, 10)
	other := make(dependencies, 5)
	self.Union(other)
}

// TestUnion tests the result of the Union operation
func TestUnion(t *testing.T) {
	self := make(dependencies, 5)
	for i := range self {
		self[i] = uint64(i)
	}

	other := make(dependencies, 5)
	for i := range other {
		other[i] = uint64(i)
	}
	same := self.Union(other)
	assert.True(t, same)

	other = make(dependencies, 5)
	for i := range other {
		other[i] = uint64(5 - i)
	}
	same = self.Union(other)

	assert.False(t, same)
	assert.Equal(t, self, dependencies{5, 4, 3, 3, 4})
}

// TestGetCopy tests the result of the GetCopy func
func TestdependenciesGetCopy(t *testing.T) {
	self := make(dependencies, 5)
	for i := range self {
		self[i] = uint64(i)
	}
	other := self.GetCopy()

	assert.True(t, &self != &other)
	assert.Equal(t, self, other)
}
