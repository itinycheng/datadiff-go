package conf

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/deckarep/golang-set/v2"
	"github.com/itinycheng/datadiff-go/common"
	"gopkg.in/yaml.v3"
)

const (
	SamplingCityHash64 = "cityHash64"
)

type Protocol clickhouse.Protocol

type StringSet struct {
	mapset.Set[string]
}

type ClickHouseConfig struct {
	Source           ClickhouseConnConfig   `yaml:"source" validate:"required"`
	Target           ClickhouseConnConfig   `yaml:"target" validate:"required"`
	DatabaseMappings []DBMappingConfig      `yaml:"database_mappings,omitempty"`
	TableMappings    []DBMappingConfig      `yaml:"table_mappings,omitempty"`
	Comparisons      []ComparisonRuleConfig `yaml:"comparison_rules"`
	ExcludeTables    ExcludeTablesConfig    `yaml:"exclude_tables,omitempty"`
	ExcludeColumns   ExcludeColumnsConfig   `yaml:"exclude_columns,omitempty"`
	ResultOutputDir  string                 `yaml:"result_output_dir,omitempty"`
}

func (*ClickHouseConfig) Validate() error {
	return nil
}

// ClickHouseConfig should implement common.JobConf interface
func (c *ClickHouseConfig) GetType() string {
	return common.ModeClickHouse
}

type ExcludeColumnsConfig struct {
	Source []string `yaml:"source,omitempty"`
	Target []string `yaml:"target,omitempty"`
}

type ExcludeTablesConfig struct {
	Source StringSet `yaml:"source,omitempty"`
	Target StringSet `yaml:"target,omitempty"`
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
	Name              string   `yaml:"name"`
	AggregateFunction string   `yaml:"aggregate_function,omitempty"`
	Where             string   `yaml:"where,omitempty"`
	Sampling          Sampling `yaml:"sampling,omitempty"`
}

// Sampling defines the configuration for table sampling.
type Sampling struct {
	Method string  `yaml:"method"`
	Ratio  float64 `yaml:"ratio"`
}

func (s *Sampling) BuildSampling() string {
	if s.Method == "" {
		return ""
	}

	if s.Method != SamplingCityHash64 {
		panic(fmt.Sprintf("Unsupported sampling method: %s", s.Method))
	}

	sampleRatio := int(1 / s.Ratio)

	var builder strings.Builder
	builder.WriteString(SamplingCityHash64)
	builder.WriteString("(__datadiff_generated_pk) % ")
	builder.WriteString(fmt.Sprintf("%d = 0", sampleRatio))
	return builder.String()
}

// ===================================================
// ================ unmarshal methods ================
// ===================================================
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

func (s *StringSet) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.SequenceNode {
		return fmt.Errorf("YAML node is not a sequence, but a %v", node.Kind)
	}

	var items []string
	if err := node.Decode(&items); err != nil {
		return fmt.Errorf("failed to decode YAML sequence into string slice: %w", err)
	}

	newSet := mapset.NewThreadUnsafeSet(items...)
	*s = StringSet{newSet}
	return nil
}
