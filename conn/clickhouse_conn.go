package conn

import (
	"database/sql"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/itinycheng/datadiff-go/conf"
	"github.com/itinycheng/datadiff-go/global"
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

func Init() {
	config := conf.ClickhouseConf

	var err error
	global.SourceConn, err = NewClickHouseConn(&config.Source)
	if err != nil {
		panic("Failed to connect to source ClickHouse: " + err.Error())
	}

	global.TargetConn, err = NewClickHouseConn(&config.Target)
	if err != nil {
		panic("Failed to connect to target ClickHouse: " + err.Error())
	}
}
