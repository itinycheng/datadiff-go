package util

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/itinycheng/data-verify/model"
)

type Order int

const (
	Asc Order = iota
	Desc
)

// TODO
func Intersect(a, b []model.TableInfo) []model.TableInfo {
	var result []model.TableInfo
	for _, m := range a {
		for _, n := range b {
			if m.Equal(&n) {
				result = append(result, m)
				break
			}
		}
	}

	return result
}

func Diff(a, b []model.TableInfo) []model.TableInfo {
	var result []model.TableInfo
	for _, m := range a {
		exists := false
		for _, n := range b {
			if m.Equal(&n) {
				exists = true
				break
			}
		}

		if !exists {
			result = append(result, m)
		}
	}

	return result
}

func IsSliceOrArray(v any) bool {
	kind := reflect.TypeOf(v).Kind()
	return kind == reflect.Array || kind == reflect.Slice
}

func ToAnySlice(v any) []any {
    val := reflect.ValueOf(v)
    if val.Kind() == reflect.Array || val.Kind() == reflect.Slice {
        slice := make([]any, val.Len())
        for i := 0; i < val.Len(); i++ {
            slice[i] = val.Index(i).Interface()
        }
        return slice
    }
    return nil
}

func SortAny(s []any, sType Order) {
	sort.Slice(s, func(i, j int) bool {
		a, b := fmt.Sprintf("%v", s[i]), fmt.Sprintf("%v", s[j])
		if sType == Asc {
			return a < b
		} else {
			return a > b
		}
	})
}
