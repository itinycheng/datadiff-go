package main

import (
	"log/slog"

	"github.com/itinycheng/datadiff-go/common"
	"github.com/itinycheng/datadiff-go/conf"
	"github.com/itinycheng/datadiff-go/service"
)

func main() {
	config, err := conf.LoadConfig()
	if err != nil {
		slog.Error("Invalid configuration", "error", err)
		return
	}

	if err := config.Validate(); err != nil {
		slog.Error("Invalid configuration", "error", err)
		return
	}

	switch config.GetType() {
	case common.ModeClickHouse:
		clickhouseConfig, ok := config.(*conf.ClickHouseConfig)
		if !ok {
			slog.Error("Invalid configuration type for ClickHouse", "error", err)
			return
		}
		clickhouseService, err := service.NewClickHouseDiffService(clickhouseConfig)
		if err != nil {
			slog.Error("Failed to create ClickHouseDiffService", "error", err)
			return
		}
		runDiff(clickhouseService, clickhouseConfig)
	}

}

func runDiff[C common.JobConf, D,  P any](
	service common.DiffService[C, D, P],
	config C,
) {
	err := service.ValidateConfig(config)
	if err != nil {
		slog.Error("Invalid configuration", "error", err)
		return
	}

	// rules, err := util.BuildComparisonRules(config.Comparisons)
	rules, err := service.ListComparisonRules(config)
	if err != nil || len(rules) == 0 {
		slog.Error("Failed to build comparison rules or no rules found", "error", err)
		return
	}

	descriptors, err := service.ListDescriptors(config)
	if err != nil || len(descriptors) == 0 {
		slog.Error("Failed to list descriptors or no descriptors found", "error", err)
		return
	}

	for idx := range descriptors {
		descriptor := &descriptors[idx]
		for rIdx := range rules {
			rule := &rules[rIdx]
			data, err := service.PrepareData(config, descriptor, rule)
			if err != nil {
				slog.Error("Failed to prepare data", "error", err)
				continue
			}
			service.Diff(&data)
			slog.Info("Diff completed", "descriptor", descriptor, "rule", rule)
		}
	}

	slog.Info("All data diff tasks completed.")
}
