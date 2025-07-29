package util

import (
	"errors"
	"strings"

	"github.com/itinycheng/datadiff-go/conf"
	"github.com/itinycheng/datadiff-go/model"
)

const (
	totalOrPartitionAggregation = "total_or_partition_aggregation"
	rowByRowComparison          = "row_by_row_comparison"
	PK                          = "__datadiff_generated_pk"
)

func BuildComparisonRules(ruleConfigs []conf.ComparisonRuleConfig) ([]model.ComparisonRule, error) {
	var rules []model.ComparisonRule
	for _, config := range ruleConfigs {
		var (
			rule model.ComparisonRule
			err  error
		)
		switch config.Name {
		case totalOrPartitionAggregation:
			rule, err = buildTotalOrPartitionAggregationRule(config)
		case rowByRowComparison:
			rule, err = buildRowByRowComparisonRule(config)
		default:
			return nil, errors.New("unsupported comparison rule: " + config.Name)
		}

		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}

	return rules, nil
}

// select (* - orderbys - partitions), concat(toString(orderbys) + toString(partition)) as pk from table where sampling(pk) / 100 = random and {{where}}
func buildRowByRowComparisonRule(config conf.ComparisonRuleConfig) (model.ComparisonRule, error) {
	return model.ComparisonRule{
		CmpType: model.ComparisonTypeRowByRow,
		BuildSQLs: func(table *model.TableInfo) model.VerifySQLs {
			var builder strings.Builder
			builder.WriteString("SELECT ")

			// columns
			columns := table.FilteredColumns()
			if len(columns) == 0 {
				builder.WriteString("'' as _null_column ")
			} else {
				builder.WriteString("`")
				builder.WriteString(strings.Join(columns, "`, `"))
				builder.WriteString("`")
			}

			var pkCols []string
			sortingKeys := splitKeys(table.GetActualSortingKey())
			if sortingKeys != nil {
				for _, col := range sortingKeys {
					if col == "" {
						continue
					}

					pkCols = append(pkCols, "toString("+col+")")
				}
			}

			partitionKeys := splitKeys(table.GetActualPartitionKey())
			if partitionKeys != nil {
				for _, col := range partitionKeys {
					if col == "" {
						continue
					}

					pkCols = append(pkCols, "toString("+col+")")
				}
			}

			if len(pkCols) > 0 {
				builder.WriteString(", concat('")
				builder.WriteString(table.Name)
				builder.WriteString("-'")
				builder.WriteString(", ")
				builder.WriteString(strings.Join(pkCols, ", "))
				builder.WriteString(") AS ")
				builder.WriteString(PK)
			} else {
				builder.WriteString(", concat(")
				builder.WriteString(strings.Join(table.Columns, ", "))
				builder.WriteString(") AS ")
				builder.WriteString(PK)
			}

			builder.WriteString(" FROM ")
			builder.WriteString(table.Database)
			builder.WriteString(".")
			builder.WriteString(table.Name)
			builder.WriteString(" WHERE ")
			builder.WriteString(config.Where)

			sampling := config.Sampling.BuildSampling()
			if sampling != "" {
				builder.WriteString(" AND ")
				builder.WriteString(sampling)
			}

			return model.VerifySQLs{
				Source: []string{builder.String()},
				Target: []string{builder.String()},
			}
		},
	}, nil
}

// Example: SELECT count() as aggregated_value, concat('table-', toString(partition_expr)) as pk from table where create_at > ” group by partition_expr
func buildTotalOrPartitionAggregationRule(config conf.ComparisonRuleConfig) (model.ComparisonRule, error) {
	if config.AggregateFunction == "" {
		return model.ComparisonRule{},
			errors.New("Aggregate function must be specified for total_or_partition_aggregation_rule")
	}

	return model.ComparisonRule{
		CmpType: model.ComparisonTypeTotalOrPartitionAggregation,
		BuildSQLs: func(table *model.TableInfo) model.VerifySQLs {
			partitionKey := table.GetActualPartitionKey()

			var builder strings.Builder
			builder.WriteString("SELECT ")
			builder.WriteString(config.AggregateFunction)
			builder.WriteString(" AS aggregated_value")
			if partitionKey != "" {
				builder.WriteString(", ")
				builder.WriteString("concat('")
				builder.WriteString(table.Name)
				builder.WriteString("-', toString(")
				builder.WriteString(partitionKey)
				builder.WriteString(")) AS ")
				builder.WriteString(PK)
			} else {
				builder.WriteString(", '")
				builder.WriteString(table.Name)
				builder.WriteString("' AS ")
				builder.WriteString(PK)
			}

			builder.WriteString(" FROM ")
			builder.WriteString(table.Database)
			builder.WriteString(".")
			builder.WriteString(table.Name)
			builder.WriteString(" WHERE ")
			builder.WriteString(config.Where)
			if partitionKey != "" {
				builder.WriteString(" GROUP BY ")
				builder.WriteString(partitionKey)
				builder.WriteString(" ORDER BY ")
				builder.WriteString(PK)
			}

			return model.VerifySQLs{
				Source: []string{builder.String()},
				Target: []string{builder.String()},
			}
		},
	}, nil
}

func splitKeys(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return SplitFields(s)
}
