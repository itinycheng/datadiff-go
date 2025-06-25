package conf

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"gopkg.in/yaml.v3"
)

var ClickHOuseConfig *ClickHouseConfig

type Protocol clickhouse.Protocol

type ClickHouseConfig struct {
	Source           ClickhouseConnConfig `yaml:"source"`
	Target           ClickhouseConnConfig `yaml:"target"`
	DatabaseMappings []DBMapping          `yaml:"database_mappings,omitempty"`
	TableMappings    []DBMapping          `yaml:"table_mappings,omitempty"`
	ComparisonRules  []ComparisonRule     `yaml:"comparison_rules"`
	ExcludeTables    []string             `yaml:"exclude_tables,omitempty"`
}

type ClickhouseConnConfig struct {
	Protocol Protocol `yaml:"protocol"`
	Addr     []string `yaml:"addr"`
	Database string   `yaml:"database"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

type DBMapping struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

type ComparisonRule struct {
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

func init() {
	if err := yaml.Unmarshal(clickhouseYaml, &ClickHOuseConfig); err != nil {
		slog.Error("failed to parse clickhouse.yaml", slog.Any("err", err))
		os.Exit(1)
	}
}
