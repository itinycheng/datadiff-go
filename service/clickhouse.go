package service

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/itinycheng/data-verify/conf"
	"github.com/itinycheng/data-verify/model"
	"github.com/itinycheng/data-verify/repo"
	"github.com/itinycheng/data-verify/util"
)

type ClickHouseVerifyService struct{}

func (service *ClickHouseVerifyService) GetVerifiableTables(dbConf conf.DBMappingConfig) ([]model.TableInfo, error) {
	sourceTables, err := findVerifiableTables(clickhouseSourceRepo, dbConf.Source)
	if err != nil {
		return nil, err
	}

	targetTables, err := findVerifiableTables(clickhouseTargetRepo, dbConf.Target)
	if err != nil {
		return nil, err
	}

	tables := util.Intersect(sourceTables, targetTables)
	sourceDiff := util.Diff(sourceTables, tables)
	targetDiff := util.Diff(targetTables, tables)
	if len(sourceDiff) > 0 || len(targetDiff) > 0 {
		slog.Info("Can not found corresponding tables", "source tables", sourceDiff, "target tables", targetDiff)
	}

	err = initLocalIfNeeded(tables)
	if err != nil {
		return nil, err
	}

	return tables, nil
}

func (service *ClickHouseVerifyService) FilterExcludedTables(tables []model.TableInfo, excludeTables conf.ExcludeTablesConfig) []model.TableInfo {
	excludeSet := make(map[string]struct{}, 0)
	if len(excludeTables.Source) > 0 {
		for _, ex := range excludeTables.Source {
			excludeSet[ex] = struct{}{}
		}
	}

	if len(excludeTables.Target) > 0 {
		for _, ex := range excludeTables.Target {
			excludeSet[ex] = struct{}{}
		}
	}

	var filtered []model.TableInfo
	for _, table := range tables {
		if _, exists := excludeSet[table.Name]; !exists {
			filtered = append(filtered, table)
		}
	}
	return filtered
}

func (service *ClickHouseVerifyService) PrepareDataForVerification(dataPool *model.DataPool) error {
	sqls := dataPool.SQLs
	for _, sql := range sqls.Source {
		maps, err := clickhouseSourceRepo.QueryRowToMap(sql)
		if err != nil || maps == nil {
			slog.Error("Failed to query source data", "sql", sql, "error", err)
			return err
		}

		for _, rowMap := range maps {
			key := rowMap[util.PK].(string) + "_" + strconv.Itoa(sqls.Id)
			dataPool.Source[key] = rowMap
		}
	}

	for _, sql := range sqls.Target {
		maps, err := clickhouseTargetRepo.QueryRowToMap(sql)
		if err != nil || maps == nil {
			slog.Error("Failed to query source data", "sql", sql, "error", err)
			return err
		}

		for _, rowMap := range maps {
			key := rowMap[util.PK].(string) + "_" + strconv.Itoa(sqls.Id)
			dataPool.Target[key] = rowMap
		}
	}

	return nil
}

func (service *ClickHouseVerifyService) Verify(data *model.DataPool) {
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
func createOutputAndSummaryFile(outputDir string, filePrefix string) (*os.File, *os.File, error) {
	if outputDir == "" {
		outputDir = "."
	}
	subDir := time.Now().Format("20060102")
	outputDir = outputDir + "/" + subDir
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, nil, err
	}
	filePath := outputDir + "/" + filePrefix + "_verify_result.txt"
	outputFile, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, nil, err
	}

	summaryFilePath := outputDir + "/summary.json"
	summaryFile, err := os.OpenFile(summaryFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	return outputFile, summaryFile, nil
}

func initLocalIfNeeded(tables []model.TableInfo) error {
	for i := range tables {
		info := &tables[i]
		if !info.IsDistributed() {
			continue
		}

		// init local table and database
		info.InitLocalTableAndDB()

		// init local partition key
		partitionKey, sortingKey, err := clickhouseSourceRepo.QueryPartitionAndSortingKey(info.LocalName, info.LocalDatabase)
		if err != nil {
			slog.Error("Failed to query partition/sorting key", "table", info.LocalName, "database", info.LocalDatabase, "error", err)
			return err
		}
		info.LocalPartitionKey = partitionKey
		info.LocalSortingKey = sortingKey
	}

	return nil
}

func findVerifiableTables(r *repo.ClickHouseRepo, database string) ([]model.TableInfo, error) {
	all, err := r.QueryAllTables(database)
	if err != nil {
		return nil, err
	}

	distrTables, err := r.QueryDistrTables(database)
	if err != nil {
		return nil, err
	}

	var tables []model.TableInfo = make([]model.TableInfo, 0, len(distrTables))
	for _, info := range distrTables {
		tables = append(tables, info)
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
			tables = append(tables, *table)
		}
	}

	// Query columns for each table
	for i := range tables {
		table := &tables[i]
		columns, err := r.QueryAllColumns(table.Database, table.Name)
		if err != nil {
			return nil, err
		}

		table.Columns = columns
	}

	return tables, nil
}
