package repo

import (
	"database/sql"
	"log/slog"

	"github.com/itinycheng/datadiff-go/model"
)

const (
	allTableColumns   = "SELECT name from system.columns WHERE database = ? AND table = ?"
	allTableEngines   = "SELECT name, database, engine, engine_full, partition_key, sorting_key FROM system.tables WHERE database = ?"
	distrTableEngines = "SELECT name, database, engine, engine_full, partition_key, sorting_key FROM system.tables WHERE database = ? AND engine = 'Distributed'"
	partitionKeyQuery = "SELECT partition_key, sorting_key FROM system.tables WHERE name = ? AND database = ? LIMIT 1"
)

type ClickHouseRepo struct {
	conn *sql.DB
}

func NewClickHouseRepo(conn *sql.DB) *ClickHouseRepo {
	return &ClickHouseRepo{conn: conn}
}

func (r *ClickHouseRepo) QueryAllColumns(database, table string) ([]string, error) {
	slog.Info("Querying all columns", "database", database, "table", table)
	rows, err := r.conn.Query(allTableColumns, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		columns = append(columns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

func (r *ClickHouseRepo) QueryRowToMap(query string, args ...any) ([]map[string]any, error) {
	slog.Info("Executing query", "query", query, "args", args)
	rows, err := r.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result = make([]map[string]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		rowMap := make(map[string]any)
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}
		result = append(result, rowMap)
	}

	return result, nil
}

func (r *ClickHouseRepo) QueryPartitionAndSortingKey(table, database string) (string, string, error) {
	slog.Info("Querying partition and sorting key", "table", table, "database", database)
	var (
		partitionKey string
		sortingKey   string
	)
	err := r.conn.QueryRow(partitionKeyQuery, table, database).Scan(&partitionKey, &sortingKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", "", nil
		}
		return "", "", err
	}
	return partitionKey, sortingKey, nil
}

func (r *ClickHouseRepo) QueryDistrTables(database string) ([]model.TableInfo, error) {
	return r.QueryTables(distrTableEngines, database)
}

func (r *ClickHouseRepo) QueryAllTables(database string) ([]model.TableInfo, error) {
	return r.QueryTables(allTableEngines, database)
}

func (r *ClickHouseRepo) QueryTables(query string, args ...any) ([]model.TableInfo, error) {
	slog.Info("Querying tables", "query", query, "args", args)
	rows, err := r.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var infos []model.TableInfo
	for rows.Next() {
		var info model.TableInfo
		if err := rows.Scan(&info.Name, &info.Database, &info.Engine,
			&info.EngineFull, &info.PartitionKey, &info.SortingKey); err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return infos, nil
}
