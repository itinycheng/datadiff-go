package conf

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"gopkg.in/yaml.v3"
)

var ClickhouseConf *ClickHouseConfig

type Protocol clickhouse.Protocol

type ClickHouseConfig struct {
	Source           ClickhouseConnConfig   `yaml:"source"`
	Target           ClickhouseConnConfig   `yaml:"target"`
	DatabaseMappings []DBMappingConfig      `yaml:"database_mappings,omitempty"`
	TableMappings    []DBMappingConfig      `yaml:"table_mappings,omitempty"`
	Comparisons      []ComparisonRuleConfig `yaml:"comparison_rules"`
	ExcludeTables    ExcludeTablesConfig    `yaml:"exclude_tables,omitempty"`
	ResultOutputDir  string                 `yaml:"result_output_dir,omitempty"`
}

type ExcludeTablesConfig struct {
	Source []string `yaml:"source,omitempty"`
	Target []string `yaml:"target,omitempty"`
}

type ClickhouseConnConfig struct {
	Protocol Protocol `yaml:"protocol"`
	Addr     []string `yaml:"addr"`
	Database string   `yaml:"database"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

type DBMappingConfig struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

type ComparisonRuleConfig struct {
	Name              string    `yaml:"name"`
	AggregateFunction string    `yaml:"aggregate_function,omitempty"`
	Where             string    `yaml:"where,omitempty"`
	Sampling          *Sampling `yaml:"sampling,omitempty"`
}

// Sampling defines the configuration for table sampling.
type Sampling struct {
	Method  string  `yaml:"method"`
	Ratio   float64 `yaml:"ratio"`
	MinRows int     `yaml:"min_rows,omitempty"`
}

func (p *Protocol) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}

	switch strings.ToLower(s) {
	case "http", "https":
		*p = Protocol(clickhouse.HTTP)
	case "native", "tcp", "":
		*p = Protocol(clickhouse.Native)
	default:
		return fmt.Errorf("unknown protocol: %s", s)
	}

	return nil
}
