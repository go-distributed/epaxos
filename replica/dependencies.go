package replica

type dependencies []uint64

// union unions the deps into the receiver
func (d dependencies) Union(other dependencies) bool {
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

func (d dependencies) GetCopy() dependencies {
	deps := make(dependencies, len(d))
	copy(deps, d)
	return deps
}
