package data

type Dependencies []uint64

// union unions the deps into the receiver
// return true if deps are changed
func (d Dependencies) Union(other Dependencies) bool {
	if d == nil || other == nil {
		panic("Union: dependencis should not be nil")
	}
	if len(d) != len(other) {
		panic("Union: size different!")
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

func (d Dependencies) Clone() Dependencies {
	if d == nil {
		return nil
	}
	deps := make(Dependencies, len(d))
	copy(deps, d)
	return deps
}

func (d Dependencies) SameAs(other Dependencies) bool {
	if len(d) != len(other) {
		panic("Same: different size!")
	}

	for i := range d {
		if d[i] != other[i] {
			return false
		}
	}
	return true
}
