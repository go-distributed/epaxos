package replica

type dependencies []uint64

// union unions the deps into the receiver
func (d dependencies) union(other dependencies) bool {
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
