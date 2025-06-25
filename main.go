package main

import (
	"flag"
	"log/slog"

	"github.com/itinycheng/data-verify/conf"
	"github.com/itinycheng/data-verify/service"
)

const (
	ModeClickHouse = "clickhouse"
)

func main() {
	mode := flag.String("mode", "clickhouse", "Mode of operation, currently only 'clickhouse' is supported")
	if mode == nil || *mode != ModeClickHouse {
		slog.Error("Unsupported mode. Currently only 'clickhouse' is supported.")
		return
	}

	config := conf.ClickHOuseConfig

	for _, mapping := range config.DatabaseMappings {
		tables, err := service.GetVerifiableTables(mapping)
		if err != nil {
			slog.Error("Failed to get verifiable tables", "error", err)
			continue
		}

		slog.Info("Verifiable tables found", "source", mapping.Source, "target", mapping.Target, "tables", tables)
	}

}
