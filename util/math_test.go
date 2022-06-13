package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMinUint64(t *testing.T) {
	var a, b uint64 = 4, 6

	require.Equal(t, a, MinUint64(a, b))
	require.Equal(t, a, MinUint64(b, a))
}

func TestMaxUint64(t *testing.T) {
	var a, b uint64 = 4, 6

	require.Equal(t, b, MaxUint64(a, b))
	require.Equal(t, b, MaxUint64(b, a))
}
