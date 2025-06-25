package service

import (
	"github.com/itinycheng/data-verify/global"
	"github.com/itinycheng/data-verify/repo"
)

var (
	clickhouseSourceRepo = repo.NewClickHouseRepo(global.SourceConn)
	clickhouseTargetRepo = repo.NewClickHouseRepo(global.SourceConn)
)
