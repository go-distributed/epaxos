package message

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

// TestClone tests the result of the Clone func
func TestDependenciesClone(t *testing.T) {
	self := make(Dependencies, 5)
	for i := range self {
		self[i] = uint64(i)
	}
	other := self.Clone()

	assert.True(t, &self != &other)
	assert.Equal(t, self, other)
}

func TestDependenciesNilClone(t *testing.T) {
	var d Dependencies
	d = nil
	assert.Nil(t, d.Clone())
}
func TestDependenciesSameAs(t *testing.T) {
	self := make(Dependencies, 5)
	for i := range self {
		self[i] = uint64(i)
	}
	other := self.Clone()
	assert.True(t, self.SameAs(other))

	for i := range other {
		other[i] = uint64(len(other) - i)
	}
	assert.False(t, self.SameAs(other))

	other = make(Dependencies, 4)
	assert.Panics(t, func() { self.SameAs(other) })
}
