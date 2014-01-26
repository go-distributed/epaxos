package data

type Dependencies []uint64

// union unions the deps into the receiver
func (d Dependencies) union(other Dependencies) bool {
	if d == nil || other == nil {
		panic("union: dependencis should not be nil")
	}
	if len(d) != len(other) {
		panic("union: size different!")
	}

	same := true
	for i := range d {
		if d[i] != other[i] {
			same = false
			if d[i] < other[i] {
				d[i] = other[i]
			}
		}
	}
	return same
}

func (d Dependencies) GetCopy() Dependencies {
	deps := make(Dependencies, len(d))
	copy(deps, d)
	return deps
}
