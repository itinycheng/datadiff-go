package tests

import (
	"cmp"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/itinycheng/datadiff-go/util"
	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	fmt.Println(reflect.DeepEqual(nil, nil))
	fmt.Println(max('a', 2))
	fmt.Println(math.Max(float64(1), 2))
	ints := []any{3, 1, 2}
	util.SortByString(ints, util.Desc)
	require.Equal(t, []any{3, 2, 1}, ints, "Sort function did not sort the slice correctly")
}

func max[T cmp.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
