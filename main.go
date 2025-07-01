package main

import (
	"log/slog"

	"github.com/itinycheng/data-verify/conf"
	"github.com/itinycheng/data-verify/conn"
	"github.com/itinycheng/data-verify/model"
	"github.com/itinycheng/data-verify/service"
	"github.com/itinycheng/data-verify/util"
)

var verifyService service.VerifyService

func main() {
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

		for _, info := range tables {
			doVerify(&info, rules)
		}
	}
}

func doVerify(table *model.TableInfo, rules []model.ComparisonRule) {
	table.ExcludeColumns = conf.ClickhouseConf.ExcludeColumns.Source
	for i := range rules {
		rule := &rules[i]
		slog.Info("Initializing data and verify", "table", table.Name)

		sqls := rule.BuildSQLs(table)
		sqls.Id = i
		if !sqls.IsValidSQL() {
			slog.Error("Invalid SQLs generated", "sqls", sqls)
			continue
		}

		data := &model.DataPool{
			SourceTable: table,
			SQLs:        &sqls,
			Source:      make(map[string]map[string]any),
			Target:      make(map[string]map[string]any),
			OutputDir:   conf.ClickhouseConf.ResultOutputDir,
		}

		verifyService.PrepareDataForVerification(data)
		verifyService.Verify(data)
		slog.Info("Rule verified", "rule", rule, "table", table.Name)
	}
}

func init() {
	conf.Init()
	conn.Init()
	service.Init()
}
