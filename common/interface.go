package common

import (
	"github.com/itinycheng/datadiff-go/model"
)

type IEqual[T any] interface {
	Equal(other *T) bool
}

type JobConf interface {
	GetType() string
	Validate() error
}

type DiffService[C, D, P any] interface {
	ValidateConfig(config C) error
	ListComparisonRules(config C) ([]model.ComparisonRule, error)
	ListDescriptors(config C) ([]D, error)
	PrepareData(config C, descriptor *D, rule *model.ComparisonRule) (P, error)
	Diff(dataPool *P)
}
