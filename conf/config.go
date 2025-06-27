package conf

import (
	_ "embed"

	"gopkg.in/yaml.v3"
)

//go:embed clickhouse.yaml
var clickhouseYaml []byte

func Init() {
	if err := yaml.Unmarshal(clickhouseYaml, &ClickhouseConf); err != nil {
		panic("Failed to parse clickhouse yaml: " + err.Error())
	}
}
