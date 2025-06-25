package service

import (
	"log/slog"
	"strings"

	"github.com/itinycheng/data-verify/conf"
	"github.com/itinycheng/data-verify/repo"
	"github.com/itinycheng/data-verify/util"
)

type ClickHouseService struct {
}

func GetVerifiableTables(dbConf conf.DBMapping) ([]string, error) {
	sourceTables, err := findVerifiableTables(clickhouseSourceRepo, dbConf.Source)
	if err != nil {
		return nil, err
	}

	targetTables, err := findVerifiableTables(clickhouseTargetRepo, dbConf.Target)
	if err != nil {
		return nil, err
	}

	return util.Intersect(sourceTables, targetTables), nil
}

// ================== private ==================

func findVerifiableTables(repo *repo.ClickHouseRepo, database string) ([]string, error) {
	all, err := repo.QueryAllTables(database)
	if err != nil {
		return nil, err
	}

	engines, err := repo.QueryDistrEngines(database)
	if err != nil {
		return nil, err
	}

	var tables []string

Outer:
	for _, table := range all {
		for _, engine := range engines {
			if strings.Contains(engine, table) {
				slog.Info("Skipping local table", "table", table, "engine", engine)
				break Outer
			}
		}

		tables = append(tables, table)
	}

	return tables, nil
}
