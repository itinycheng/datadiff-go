package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"github.com/itinycheng/datadiff-go/common"
	"github.com/itinycheng/datadiff-go/conf"
	"github.com/itinycheng/datadiff-go/conn"
	"github.com/itinycheng/datadiff-go/model"
	"github.com/itinycheng/datadiff-go/repo"
	"github.com/itinycheng/datadiff-go/util"
	"github.com/spf13/cast"
)

type ClickHouseDiffService struct {
	sourceRepo *repo.ClickHouseRepo
	targetRepo *repo.ClickHouseRepo
}

func NewClickHouseDiffService(config *conf.ClickHouseConfig) (*ClickHouseDiffService, error) {
	sourceConn, err := conn.NewClickHouseConn(&config.Source)
	if err != nil {
		return nil, fmt.Errorf("Failed to create source connection: %w", err)
	}

	targetConn, err := conn.NewClickHouseConn(&config.Target)
	if err != nil {
		return nil, fmt.Errorf("Failed to create target connection: %w", err)
	}

	return &ClickHouseDiffService{
		sourceRepo: repo.NewClickHouseRepo(sourceConn),
		targetRepo: repo.NewClickHouseRepo(targetConn),
	}, nil
}

func (service *ClickHouseDiffService) ValidateConfig(config *conf.ClickHouseConfig) error {
	if config == nil {
		return errors.New("config is nil")
	}

	if len(config.Comparisons) == 0 {
		return errors.New("comparison rules are not configured")
	}

	return nil
}

func (service *ClickHouseDiffService) ListComparisonRules(config *conf.ClickHouseConfig) ([]model.ComparisonRule, error) {
	return buildComparisonRules(config.Comparisons)
}

func (service *ClickHouseDiffService) ListDescriptors(config *conf.ClickHouseConfig) ([]model.TableInfo, error) {
	mapping := config.DatabaseMappings
	if len(mapping) == 0 {
		return nil, errors.New("database mapping is not configured")
	}

	var results []model.TableInfo
	for _, dbConf := range mapping {
		// fetching table info list
		sourceTables, err := fetchTableInfos(service.sourceRepo, dbConf.Source)
		if err != nil {
			return nil, err
		}

		targetTables, err := fetchTableInfos(service.targetRepo, dbConf.Target)
		if err != nil {
			return nil, err
		}

		// excluding tables
		sourceTables = excludingTablesByName(sourceTables, config.ExcludeTables.Source)
		targetTables = excludingTablesByName(targetTables, config.ExcludeTables.Target)

		tables := util.Intersect(sourceTables, targetTables)
		sourceDiff := util.Diff(sourceTables, tables)
		targetDiff := util.Diff(targetTables, tables)
		if len(sourceDiff) > 0 || len(targetDiff) > 0 {
			slog.Warn("Can not found corresponding tables", "source tables", sourceDiff, "target tables", targetDiff)
		}

		err = initLocalIfNeeded(service.sourceRepo, tables)
		if err != nil {
			return nil, err
		}

		for _, t := range tables {
			results = append(results, *t)
		}
	}

	return results, nil
}

func (service *ClickHouseDiffService) PrepareData(config *conf.ClickHouseConfig, descriptor *model.TableInfo, rule *model.ComparisonRule) (model.DataPool, error) {
	sqls := rule.BuildSQLs(descriptor)
	sourceCh := make(chan rowData, 1)
	targetCh := make(chan rowData, 1)

	go getDataMap(sqls.Id, sqls.Source, service.sourceRepo, sourceCh)
	go getDataMap(sqls.Id, sqls.Target, service.targetRepo, targetCh)
	sourceResult := <-sourceCh
	targetResult := <-targetCh

	if sourceResult.Err != nil || targetResult.Err != nil {
		return model.DataPool{}, errors.Join(sourceResult.Err, targetResult.Err)
	}

	return model.DataPool{
		SourceTable: descriptor,
		SQLs:        &sqls,
		Source:      sourceResult.Data,
		Target:      targetResult.Data,
		OutputDir:   config.ResultOutputDir,
	}, nil
}

func (service *ClickHouseDiffService) Diff(data *model.DataPool) {
	tableName := data.SourceTable.Name
	slog.Info("Starting verification", "table", tableName)
	o, s, err := createOutputAndSummaryFile(data.OutputDir, tableName)
	if err != nil {
		slog.Error("Failed to create result file", "error", err)
		return
	}
	defer o.Close()
	defer s.Close()

	var mismatched int
	for k, v := range data.Source {
		tv := data.Target[k]
		if util.DeepEqual(v, tv) {
			slog.Debug("Data is equal", "key", k, "source", v, "target", tv)
			continue
		}

		mismatched++
		sBytes, _ := json.Marshal(v)
		tBytes, _ := json.Marshal(tv)
		o.WriteString("Key: " + k + ", Source: " + string(sBytes) + ", Target: " + string(tBytes) + "\n")
	}

	sourceRows := len(data.Source)
	targetRows := len(data.Target)
	mismatchRatio := float64(mismatched) / math.Max(float64(sourceRows), 1.0)

	var builder strings.Builder
	builder.WriteString("Table: " + tableName)
	builder.WriteString("\n		SQLs: " + data.SQLs.String())
	builder.WriteString("\n		Source rows: " + strconv.Itoa(sourceRows))
	builder.WriteString("\n		Target rows: " + strconv.Itoa(targetRows))
	builder.WriteString("\n		Mismatches: " + fmt.Sprintf("%.5f", mismatchRatio))
	s.WriteString(builder.String() + "\n")

	slog.Info("Verification results written", "file", o.Name())
}

// ================== private ==================

type rowData struct {
	Data map[string]map[string]any
	Err  error
}

func getDataMap(id int, sqls []string, repo *repo.ClickHouseRepo, ch chan rowData) {
	result := make(map[string]map[string]any)
	for _, sql := range sqls {
		maps, err := repo.QueryRowToMap(sql)
		if err != nil {
			ch <- rowData{Data: nil, Err: err}
			return
		}

		for _, rowMap := range maps {
			key := rowMap[common.PK].(string) + "_" + cast.ToString(id)
			result[key] = rowMap
		}
	}
	ch <- rowData{Data: result, Err: nil}
}

func fetchTableInfos(r *repo.ClickHouseRepo, database string) ([]*model.TableInfo, error) {
	all, err := r.QueryAllTables(database)
	if err != nil {
		return nil, err
	}

	distrTables, err := r.QueryDistrTables(database)
	if err != nil {
		return nil, err
	}

	var tables []*model.TableInfo = make([]*model.TableInfo, 0, len(distrTables))
	for idx := range distrTables {
		tables = append(tables, &distrTables[idx])
	}

	for i := range all {
		table := &all[i]
		if table.IsDistributed() {
			continue
		}

		skip := false
		for j := range distrTables {
			distr := &distrTables[j]
			if strings.Contains(distr.EngineFull, table.Name) {
				slog.Debug("Skipping table", "table", table.Name, "engine_full", distr.EngineFull)
				skip = true
				break
			}
		}

		if !skip {
			tables = append(tables, table)
		}
	}

	// Query columns for each table
	for _, table := range tables {
		columns, err := r.QueryAllColumns(table.Database, table.Name)
		if err != nil {
			return nil, err
		}

		table.Columns = columns
	}

	return tables, nil
}
