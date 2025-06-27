package tests

import (
	"testing"

	"github.com/itinycheng/data-verify/util"
	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	ints := []any{3, 1, 2}
	util.SortAny(ints, util.Desc)
	require.Equal(t, []any{3, 2, 1}, ints, "Sort function did not sort the slice correctly")
}
