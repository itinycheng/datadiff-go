package tests

import (
	"reflect"
	"testing"

	"github.com/itinycheng/datadiff-go/util"
	"github.com/stretchr/testify/require"
)

func TestSplitFields(t *testing.T) {
	s := "CounterID, EventTime"
	require.True(t, reflect.DeepEqual([]string{"CounterID", "EventTime"}, util.SplitFields(s)))

	s = "hash(CounterID + 1, 2), EventDate"
	require.True(t, reflect.DeepEqual([]string{"hash(CounterID + 1, 2)", "EventDate"}, util.SplitFields(s)))

	s = "sum(a, b), count(*), id"
	require.True(t, reflect.DeepEqual([]string{"sum(a, b)", "count(*)", "id"}, util.SplitFields(s)))
}
