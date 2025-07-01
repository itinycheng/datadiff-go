package conf

import (
	_ "embed"
	"flag"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	ModeClickHouse = "clickhouse"
	ModeHdfs       = "hdfs"

	defaultConfigFile = "conf/clickhouse.yaml"
)

// //go:embed clickhouse_prod.yaml
// var clickhouseYaml []byte

func Init() {
	mode := flag.String("mode", ModeClickHouse, "Mode of operation, currently only 'clickhouse' is supported")
	configFile := flag.String("config", defaultConfigFile, "Path to the config file (optional)")
	flag.Parse()

	if mode == nil || *mode != ModeClickHouse {
		panic("Unsupported mode. Currently only 'clickhouse' is supported.")
	}
	if configFile == nil || *configFile == "" {
		panic("Config file path is required. Use -config to specify the path.")
	}

	file, err := os.Open(*configFile)
	if err != nil {
		panic("Error opening config file: " + err.Error())
	}
	defer file.Close()

	var clickhouseConf ClickHouseConfig
	if err := yaml.NewDecoder(file).Decode(&clickhouseConf); err != nil {
		panic("Error decoding config file: " + err.Error())
	}

	ClickhouseConf = &clickhouseConf
	slog.Info("Configuration loaded successfully",
		"mode", *mode,
		"configFile", *configFile)
}
