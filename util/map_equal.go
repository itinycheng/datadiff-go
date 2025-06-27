package util

import "reflect"

func DeepEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}

		if v == bv {
			return true
		}

		if IsSliceOrArray(v) {
			vSlice := ToAnySlice(v)
			bvSlice := ToAnySlice(bv)
			if len(vSlice) != len(bvSlice) {
				return false
			}

			SortAny(vSlice, Asc)
			SortAny(bvSlice, Asc)
			if !reflect.DeepEqual(vSlice, bvSlice) {
				return false
			}
			continue
		}

		if !reflect.DeepEqual(v, bv) {
			return false
		}
	}
	return true
}
