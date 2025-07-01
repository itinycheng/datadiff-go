package service

import (
	"github.com/itinycheng/datadiff-go/conf"
	"github.com/itinycheng/datadiff-go/global"
	"github.com/itinycheng/datadiff-go/model"
	"github.com/itinycheng/datadiff-go/repo"
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
