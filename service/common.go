package service

import (
	"github.com/itinycheng/data-verify/conf"
	"github.com/itinycheng/data-verify/global"
	"github.com/itinycheng/data-verify/model"
	"github.com/itinycheng/data-verify/repo"
)

var (
	clickhouseSourceRepo *repo.ClickHouseRepo
	clickhouseTargetRepo *repo.ClickHouseRepo
)

type VerifyService interface {
	GetVerifiableTables(dbConf conf.DBMappingConfig) ([]model.TableInfo, error)
	FilterExcludedTables(tables []model.TableInfo, excludeTables conf.ExcludeTablesConfig) []model.TableInfo
	PrepareDataForVerification(dataPool *model.DataPool) error
	Verify(dataPool *model.DataPool)
}

func Init() {
	clickhouseSourceRepo = repo.NewClickHouseRepo(global.SourceConn)
	clickhouseTargetRepo = repo.NewClickHouseRepo(global.TargetConn)
}
