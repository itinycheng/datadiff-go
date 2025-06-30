package model

import (
	"regexp"
	"slices"
	"strings"
)

var distributedPattern = regexp.MustCompile(`Distributed\([^,]+,\s*(?P<local_db>[^,]+),\s*(?P<local_table>[^,]+),`)

type TableInfo struct {
	Name         string `json:"name"`
	Database     string `json:"database"`
	Engine       string `json:"engine"`
	EngineFull   string `json:"engine_full"`
	PartitionKey string `json:"partition_key"`
	SortingKey   string `json:"sorting_key"`
	Columns      []string

	LocalName         string
	LocalDatabase     string
	LocalPartitionKey string
	LocalSortingKey   string

	SortingKeys    []string
	PartitionKeys  []string
	ExcludeColumns []string
}

func (t *TableInfo) IsDistributed() bool {
	return t.Engine == "Distributed"
}

func (t *TableInfo) HasPartitionKey() bool {
	key := t.GetActualPartitionKey()
	return strings.TrimSpace(key) != ""
}

func (t *TableInfo) HasSortingKey() bool {
	key := t.GetActualSortingKey()
	return strings.TrimSpace(key) != ""
}

func (t *TableInfo) GetActualPartitionKey() string {
	if t.IsDistributed() {
		return t.LocalPartitionKey
	}
	return t.PartitionKey
}

func (t *TableInfo) GetActualSortingKey() string {
	if t.IsDistributed() {
		return t.LocalSortingKey
	}
	return t.SortingKey
}

func (t *TableInfo) FilteredColumns() []string {
	if len(t.Columns) == 0 {
		return nil
	}

	partitionKey := t.GetActualPartitionKey()
	sortingKey := t.GetActualSortingKey()

	filtered := make([]string, 0, len(t.Columns))
	for _, col := range t.Columns {
		if strings.Contains(partitionKey, col) || strings.Contains(sortingKey, col) || slices.Contains(t.ExcludeColumns, col) {
			continue
		}
		filtered = append(filtered, col)
	}
	return filtered
}

func (t *TableInfo) InitLocalTableAndDB() {
	if !t.IsDistributed() || t.LocalDatabase != "" || t.LocalName != "" {
		return
	}

	info := parseDistributedEngine(t.EngineFull)
	t.LocalDatabase = strings.ReplaceAll(info["local_db"], "'", "")
	t.LocalName = strings.ReplaceAll(info["local_table"], "'", "")
}

func (this *TableInfo) Equal(other *TableInfo) bool {
	return this.Name == other.Name && this.Database == other.Database
}

// =============== private functions =================

func parseDistributedEngine(engineFull string) map[string]string {
	match := distributedPattern.FindStringSubmatch(engineFull)
	if len(match) == 0 {
		return map[string]string{}
	}

	result := make(map[string]string)
	for i, name := range distributedPattern.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}
	return result
}
