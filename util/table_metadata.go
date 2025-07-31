package util

import "strings"

func SplitFields(s string) []string {
	var fields []string
	var cur strings.Builder
	depth := 0

	for _, r := range s {
		switch r {
		case '(':
			depth++
			cur.WriteRune(r)
		case ')':
			depth--
			cur.WriteRune(r)
		case ',':
			if depth == 0 {
				fields = append(fields, strings.TrimSpace(cur.String()))
				cur.Reset()
			} else {
				cur.WriteRune(r)
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		fields = append(fields, strings.TrimSpace(cur.String()))
	}
	return fields
}
