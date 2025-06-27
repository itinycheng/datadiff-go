package model

import (
	"regexp"
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

	LocalName         string
	LocalDatabase     string
	LocalPartitionKey string
	LocalSortingKey   string

	SortingKeys []string
	PartitionKeys []string
}

func (t *TableInfo) IsDistributed() bool {
	return t.Engine == "Distributed"
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
