package main

import (
	"flag"
	"log/slog"

	"github.com/itinycheng/data-verify/conf"
	"github.com/itinycheng/data-verify/conn"
	"github.com/itinycheng/data-verify/model"
	"github.com/itinycheng/data-verify/service"
	"github.com/itinycheng/data-verify/util"
)

const (
	ModeClickHouse = "clickhouse"
	ModeHdfs       = "hdfs"
)

var verifyService service.VerifyService

func main() {
	mode := flag.String("mode", "clickhouse", "Mode of operation, currently only 'clickhouse' is supported")
	if mode == nil || *mode != ModeClickHouse {
		slog.Error("Unsupported mode. Currently only 'clickhouse' is supported.")
		return
	}

	config := conf.ClickhouseConf
	rules, err := util.BuildComparisonRules(config.Comparisons)
	if err != nil {
		slog.Error("Failed to build comparison rules", "error", err)
		return
	}

	// Initialize verify service.
	verifyService = &service.ClickHouseVerifyService{}

	for _, mapping := range config.DatabaseMappings {
		// Get verifiable tables.
		tables, err := verifyService.GetVerifiableTables(mapping)
		if err != nil || len(tables) == 0 {
			slog.Error("Failed to get verifiable tables", "error", err)
			continue
		}

		tables = verifyService.FilterExcludedTables(tables, config.ExcludeTables)
		slog.Info("Tables to be verified", "source", mapping.Source, "target", mapping.Target, "tables", tables)

		// Initialize the data pool for verification
		for _, info := range tables {
			slog.Info("Initializing data and verify", "table", info)
			data := &model.DataPool{
				SourceTable: info,
				SourceDb:    mapping.Source,
				TargetDb:    mapping.Target,
				Source:      make(map[string]map[string]any),
				Target:      make(map[string]map[string]any),
				Rules:       rules,
				OutputDir:   config.ResultOutputDir,
			}
			verifyService.PrepareDataForVerification(data)
			verifyService.Verify(data)
		}
	}
}

func init() {
	conf.Init()
	conn.Init()
	service.Init()
}
