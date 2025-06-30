package model

import "fmt"

// pk = order by + partition
// pk -> columns
type ComparisonType int

const (
	ComparisonTypeTotalOrPartitionAggregation ComparisonType = iota
	ComparisonTypeRowByRow
)

type VerifySQLs struct {
	Id     int
	Source []string
	Target []string
}

func (v *VerifySQLs) IsValidSQL() bool {
	return len(v.Source) > 0 && len(v.Target) > 0
}

func (t VerifySQLs) String() string {
	return fmt.Sprintf("Source: %v, Target: %v", t.Source, t.Target)
}

type ComparisonRule struct {
	CmpType   ComparisonType
	BuildSQLs func(table *TableInfo) VerifySQLs
}

type DataPool struct {
	SourceTable *TableInfo
	SQLs        *VerifySQLs
	Source      map[string]map[string]any
	Target      map[string]map[string]any
	OutputDir   string
}
