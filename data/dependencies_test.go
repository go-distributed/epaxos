package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnionNilPanic(t *testing.T) {
	self := new(Dependencies)
	assert.Panics(t, func() { self.Union(nil) })
}

func TestUnionSizePanic(t *testing.T) {
	self := make(Dependencies, 10)
	other := make(Dependencies, 5)
	assert.Panics(t, func() { self.Union(other) })
}

// TestUnion tests the result of the Union operation
func TestUnion(t *testing.T) {
	self := make(Dependencies, 5)
	for i := range self {
		self[i] = uint64(i)
	}

	other := make(Dependencies, 5)
	for i := range other {
		other[i] = uint64(i)
	}
	same := self.Union(other)
	assert.True(t, same)

	other = make(Dependencies, 5)
	for i := range other {
		other[i] = uint64(5 - i)
	}
	same = self.Union(other)

	assert.False(t, same)
	assert.Equal(t, self, Dependencies{5, 4, 3, 3, 4})
}

// TestGetCopy tests the result of the GetCopy func
func TestDependenciesGetCopy(t *testing.T) {
	self := make(Dependencies, 5)
	for i := range self {
		self[i] = uint64(i)
	}
	other := self.GetCopy()

	assert.True(t, &self != &other)
	assert.Equal(t, self, other)
}

func TestDependenciesNilPanic(t *testing.T) {
	var d Dependencies
	d = nil
	assert.Panics(t, func() { d.GetCopy() })
}
