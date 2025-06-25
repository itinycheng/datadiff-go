package global

import "database/sql"

var (
	SourceConn *sql.DB
	TargetConn *sql.DB
)