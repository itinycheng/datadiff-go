package util

import (
	"log/slog"
	"reflect"
	"sort"

	"github.com/itinycheng/datadiff-go/model"
	"github.com/spf13/cast"
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
	if v == nil {
		return false
	}

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

func SortByString(s []any, sType Order) {
	sort.Slice(s, func(i, j int) bool {
		a, err := cast.ToStringE(s[i])
		if err != nil {
			slog.Warn("SortByString: failed to convert value to string", "value", s[i], "error", err)
		}

		b, err := cast.ToStringE(s[j])
		if err != nil {
			slog.Warn("SortByString: failed to convert value to string", "value", s[j], "error", err)
		}
		if sType == Asc {
			return a < b
		} else {
			return a > b
		}
	})
}
