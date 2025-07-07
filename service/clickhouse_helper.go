package service

import (
	"log/slog"
	"os"
	"time"

	"github.com/itinycheng/datadiff-go/conf"
	"github.com/itinycheng/datadiff-go/model"
	"github.com/itinycheng/datadiff-go/repo"
)

func excludingTablesByName(tables []*model.TableInfo, exclusions conf.StringSet) []*model.TableInfo {
	var filtered []*model.TableInfo
	for _, table := range tables {
		if !exclusions.Contains(table.Name) {
			filtered = append(filtered, table)
		}
	}
	return filtered
}

func initLocalIfNeeded(r *repo.ClickHouseRepo, tables []*model.TableInfo) error {
	for _, info := range tables {
		if !info.IsDistributed() {
			continue
		}

		// init local table and database
		info.InitLocalTableAndDB()

		// init local partition key
		partitionKey, sortingKey, err := r.QueryPartitionAndSortingKey(info.LocalName, info.LocalDatabase)
		if err != nil {
			slog.Error("Failed to query partition/sorting key", "table", info.LocalName, "database", info.LocalDatabase, "error", err)
			return err
		}
		info.LocalPartitionKey = partitionKey
		info.LocalSortingKey = sortingKey
	}

	return nil
}

func createOutputAndSummaryFile(outputDir string, filePrefix string) (*os.File, *os.File, error) {
	if outputDir == "" {
		outputDir = "."
	}
	subDir := time.Now().Format("20060102")
	outputDir = outputDir + "/" + subDir
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, nil, err
	}
	filePath := outputDir + "/" + filePrefix + "_diff_result.txt"
	outputFile, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, nil, err
	}

	summaryFilePath := outputDir + "/summary.json"
	summaryFile, err := os.OpenFile(summaryFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	return outputFile, summaryFile, nil
}
