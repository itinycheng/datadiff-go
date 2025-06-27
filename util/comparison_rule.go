package util

import (
	"errors"
	"strings"

	"github.com/itinycheng/data-verify/conf"
	"github.com/itinycheng/data-verify/model"
)

const (
	totalOrPartitionAggregation = "total_or_partition_aggregation"
	rowByRowComparison          = "row_by_row_comparison"
	PK                          = "pk"
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

//select (* - orderbys - partitions), concat(toString(orderbys) + toString(partition)) as pk from table where sampling(pk) / 100 = random and {{where}} 
func buildRowByRowComparisonRule(config conf.ComparisonRuleConfig) (model.ComparisonRule, error) {
	return model.ComparisonRule{
		CmpType: model.ComparisonTypeRowByRow,
		BuildSQLs: func(table *model.TableInfo) model.VerifySQLs {
			
			return model.VerifySQLs{}
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
			var partitionKey string
			if table.IsDistributed() {
				partitionKey = table.LocalPartitionKey
			} else {
				partitionKey = table.PartitionKey
			}

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
