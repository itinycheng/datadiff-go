package model

// pk = order by + partition
// pk -> columns
type ComparisonType int

const (
	ComparisonTypeTotalOrPartitionAggregation ComparisonType = iota
	ComparisonTypeRowByRow
)

type VerifySQLs struct {
	Source []string
	Target []string
}

type ComparisonRule struct {
	CmpType   ComparisonType
	BuildSQLs func(table *TableInfo) VerifySQLs
}

type DataPool struct {
	SourceTable TableInfo
	SourceDb    string
	TargetDb    string
	Source      map[string]map[string]any
	Target      map[string]map[string]any
	Rules       []ComparisonRule
	OutputDir   string
}
