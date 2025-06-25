package conf

import (
	_ "embed"
)

//go:embed clickhouse.yaml
var clickhouseYaml []byte

