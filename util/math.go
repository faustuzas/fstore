package util

func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
