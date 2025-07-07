package conf

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/itinycheng/datadiff-go/common"
	"gopkg.in/yaml.v3"
)

func LoadConfig() (common.JobConf, error) {
	mode := flag.String("mode", common.ModeClickHouse, "Mode of operation, currently only 'clickhouse' is supported")
	configFile := flag.String("config", common.DefaultConfigFile, "Path to the config file (optional)")
	flag.Parse()

	if mode == nil || *mode != common.ModeClickHouse {
		return nil, fmt.Errorf("Unsupported mode. Currently only 'clickhouse' is supported.")
	}
	if configFile == nil || *configFile == "" {
		return nil, fmt.Errorf("Config file path is required. Use -config to specify the path.")
	}

	file, err := os.Open(*configFile)
	if err != nil {
		return nil, fmt.Errorf("Error opening config file: %w", err)
	}
	defer file.Close()

	var config common.JobConf
	switch *mode {
	case common.ModeClickHouse:
		var clickhouseConf ClickHouseConfig
		if err := yaml.NewDecoder(file).Decode(&clickhouseConf); err != nil {
			return nil, fmt.Errorf("Error decoding ClickHouse config file: %w", err)
		}
		config = &clickhouseConf
	case common.ModeMysql:
	case common.ModeHdfs:
		return nil, fmt.Errorf("Unsupported mode: %s", *mode)
	}

	slog.Info("Configuration loaded successfully",
		"mode", *mode,
		"configFile", *configFile)
	return config, nil
}
