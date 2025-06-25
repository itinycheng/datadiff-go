package conn

import (
	"database/sql"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/itinycheng/data-verify/conf"
)

func NewClickHouseConn(config *conf.ClickhouseConnConfig) (*sql.DB, error) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: config.Addr,
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 30 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Protocol: clickhouse.HTTP,
	})
	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	return conn, nil
}
