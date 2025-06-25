package util

// TODO: Reduce space complexity.
func Intersect(a, b []string) []string {
	m := make(map[string]struct{})
	for _, v := range a {
		m[v] = struct{}{}
	}
	var result []string
	for _, v := range b {
		if _, ok := m[v]; ok {
			result = append(result, v)
		}
	}
	return result
}

// TODO: Reduce space complexity.
func Diff(a, b []string) []string {
	m := make(map[string]struct{}, len(b))
	for _, v := range b {
		m[v] = struct{}{}
	}
	var result []string
	for _, v := range a {
		if _, ok := m[v]; !ok {
			result = append(result, v)
		}
	}
	return result
}
