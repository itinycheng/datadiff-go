package repo

import (
	"database/sql"
)

const (
	allTables         = "SELECT name FROM system.tables WHERE database = ?"
	distrTableEngines = "SELECT engine_full FROM system.tables WHERE database = ? AND engine = 'Distributed'"
)

type ClickHouseRepo struct {
	conn *sql.DB
}

func NewClickHouseRepo(conn *sql.DB) *ClickHouseRepo {
	return &ClickHouseRepo{conn: conn}
}

func (r *ClickHouseRepo) QueryAllTables(database string) ([]string, error) {
	rows, err := r.conn.Query(allTables, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

func (r *ClickHouseRepo) QueryDistrEngines(database string) ([]string, error) {
	rows, err := r.conn.Query(distrTableEngines, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var engines []string
	for rows.Next() {
		var engine string
		if err := rows.Scan(&engine); err != nil {
			return nil, err
		}
		engines = append(engines, engine)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return engines, nil
}
