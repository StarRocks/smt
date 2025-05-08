package convert

import (
	"fmt"
	"math"
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/model"
	"starrocks-migrate-tool/source"
	"strconv"
	"strings"
	"time"

	funk "github.com/thoas/go-funk"
)

type StarRocks struct {
	Converter
}

func (c *StarRocks) Construct(config *conf.Config, dbProvider source.IDBSourceProvider) IConverter {
	c.dbProvider = dbProvider
	c.config = config
	return c
}

func (c *StarRocks) ResultFilePrefix() string {
	return "starrocks-create"
}

func (c *StarRocks) ToCreateDDL() ([]string, map[string][]string, error) {
	ddlList := []string{}
	ruledDDLMap := map[string][]string{}
	for matchedTableRule, tableColumnsList := range c.dbProvider.GetRuledTablesMap() {
		for _, tableColumns := range tableColumnsList {
			partitionKey := ""
			if _, ok := ruledDDLMap[matchedTableRule.Seq]; !ok {
				ruledDDLMap[matchedTableRule.Seq] = []string{}
			}
			if len(matchedTableRule.PartitionKey) > 0 {
				partitionKey = matchedTableRule.PartitionKey
			}
			databaseName := tableColumns.Table.TABLE_CATALOG
			ddl := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", databaseName)
			if !funk.ContainsString(ddlList, ddl) {
				ddlList = append(ddlList, ddl)
			}
			if !funk.ContainsString(ruledDDLMap[matchedTableRule.Seq], ddl) {
				ruledDDLMap[matchedTableRule.Seq] = append(ruledDDLMap[matchedTableRule.Seq], ddl)
			}
			shemaPrefixedTableName := tableColumns.Table.GetSchemaPrefixedTableName()
			if !matchedTableRule.FromShardingSrc && !c.dbProvider.CombineSchemaName() {
				shemaPrefixedTableName = tableColumns.Table.TABLE_NAME
			}
			createTableDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n", databaseName, shemaPrefixedTableName)
			columnStrList := []string{}
			keys := []string{}
			if len(tableColumns.PrimaryKCU) > 0 {
				keys, tableColumns.Columns = c.reorderTableColumns(tableColumns.PrimaryKCU, tableColumns.Columns)
			} else if len(tableColumns.UniqueKCU) > 0 {
				// unique keys as primary keys
				keys, tableColumns.Columns = c.reorderTableColumns(tableColumns.UniqueKCU, tableColumns.Columns)
			}
			// 1. concat columns
			for _, column := range tableColumns.Columns {
				columnStr, err := c.dbProvider.FormatStarRocksColumnDef(tableColumns.Table, column)
				if err != nil {
					return ddlList, ruledDDLMap, err
				}
				columnStrList = append(columnStrList, columnStr)
				if column.DATA_TYPE != "date" && column.DATA_TYPE != "datetime" && column.DATA_TYPE != "timestamp" {
					continue
				}
				if len(partitionKey) == 0 {
					partitionKey = column.COLUMN_NAME
				}
			}
			createTableDDL += strings.Join(columnStrList, ",\n") + fmt.Sprintf("\n) ENGINE=olap\n")

			// 2. concat keys
			keysList := ""
			if len(keys) > 0 {
				keysList = strings.Join(funk.Map(keys, func(key string) string {
					return fmt.Sprintf("`%s`", key)
				}).([]string), ", ")
				if c.config.DBType == common.DBSourceHive || (c.config.DBType == common.DBSourceClickHouse && tableColumns.Table.ENGINE == "MergeTree") {
					createTableDDL += fmt.Sprintf("DUPLICATE KEY(%s)\n", keysList)
				} else if c.config.DBType == common.DBSourceClickHouse && tableColumns.Table.ENGINE == "SummingMergeTree" {
					createTableDDL += fmt.Sprintf("AGGREGATE KEY(%s)\n", keysList)
				} else {
					createTableDDL += fmt.Sprintf("PRIMARY KEY(%s)\n", keysList)
				}
			} else {
				dupKeys := funk.Map(tableColumns.Columns, func(col *model.Column) string {
					return fmt.Sprintf("`%s`", col.COLUMN_NAME)
				}).([]string)
				if len(dupKeys) > 3 {
					dupKeys = dupKeys[:3]
				}
				keysList = strings.Join(dupKeys, ", ")
				if len(matchedTableRule.DuplicateKeys) > 0 {
					createTableDDL += fmt.Sprintf("DUPLICATE KEY(%s)\n", matchedTableRule.DuplicateKeys)
				} else {
					createTableDDL += fmt.Sprintf("DUPLICATE KEY(%s)\n", keysList)
				}
			}
			// 3. concat comment
			createTableDDL += fmt.Sprintf("COMMENT \"%s\"\n", tableColumns.Table.TABLE_COMMENT)

			// 4. concat partitions
			partitionSize, dynamicProperties, partitions := c.calculatePartitions(int64(tableColumns.Table.DATA_LENGTH), tableColumns.Table.CREATE_TIME)
			if len(keys) == 0 && len(partitionKey) > 0 && len(partitions) > 0 {
				// only duplicate keys got partitions
				if len(matchedTableRule.Partitions) > 0 {
					partitions = matchedTableRule.Partitions
				}
				createTableDDL += fmt.Sprintf("PARTITION BY RANGE (%s) (\n%s\n)\n", partitionKey, partitions)
			}

			// 5. concat distributed buckets
			disKeys := keysList
			if len(matchedTableRule.DistributedBy) > 0 {
				disKeys = matchedTableRule.DistributedBy
			}
			buckets := c.calculateBuckets(partitionSize)
			if matchedTableRule.Buckets > 0 {
				buckets = matchedTableRule.Buckets
			}
			createTableDDL += fmt.Sprintf("DISTRIBUTED BY HASH(%s) BUCKETS %d\n", disKeys, buckets)

			// 6. concat properties
			properties := matchedTableRule.Properties
			if _, ok := properties["dynamic_partition.time_unit"]; len(keys) == 0 && !ok {
				if len(dynamicProperties) > 0 {
					for k, v := range dynamicProperties {
						properties[k] = v
					}
					properties["dynamic_partition.buckets"] = strconv.FormatInt(buckets, 10)
				}
			}
			propsArr := []string{}
			for key, val := range properties {
				propsArr = append(propsArr, fmt.Sprintf("  \"%s\" = \"%s\"", key, val))
			}
			createTableDDL += fmt.Sprintf("PROPERTIES (\n%s\n)", strings.Join(propsArr, ",\n"))
			ddlList = append(ddlList, createTableDDL)
			ruledDDLMap[matchedTableRule.Seq] = append(ruledDDLMap[matchedTableRule.Seq], createTableDDL)
		}
	}
	return ddlList, ruledDDLMap, nil
}

func (c *StarRocks) calculatePartitions(tableSize int64, tableCreatedTime time.Time) (partitionSize int64, dProps map[string]string, partitions string) {
	dynamicProperties := map[string]string{}
	setDProps := func(interval string) {
		dynamicProperties["dynamic_partition.enable"] = "true"
		dynamicProperties["dynamic_partition.time_unit"] = interval
		dynamicProperties["dynamic_partitoin.end"] = "3"
		dynamicProperties["dynamic_partition.prefix"] = "auto_gen_p_"
	}
	days := int64(math.Ceil(float64(time.Now().Unix()-tableCreatedTime.Unix()) / float64(common.DAY_SECONDS)))
	if days == 0 {
		days = 1
	}
	if tableSize/days > 10*common.GIGA_BYTES {
		// partition by day
		if tableSize < 100*common.GIGA_BYTES {
			return tableSize / days, dynamicProperties, ""
		}
		setDProps("DAY")
		return tableSize / days, dynamicProperties, fmt.Sprintf("  START (\"%s\") END (\"%s\") EVERY (INTERVAL 1 day)", tableCreatedTime.Format(common.DATE_TEMPLATE), time.Unix(time.Now().Unix()+common.DAY_SECONDS, 0).Format(common.DATE_TEMPLATE))
	}
	if tableSize/days > common.GIGA_BYTES {
		// partition by month
		if tableSize < 100*common.GIGA_BYTES {
			return tableSize / days * 30, dynamicProperties, ""
		}
		setDProps("MONTH")
		return tableSize / days * 30, dynamicProperties, fmt.Sprintf("  START (\"%s\") END (\"%s\") EVERY (INTERVAL 1 month)", tableCreatedTime.Format(common.DATE_TEMPLATE)[:7]+"-01", time.Now().AddDate(0, 1, 0).Format(common.DATE_TEMPLATE)[:7]+"-01")
	}
	if tableSize < 100*common.GIGA_BYTES {
		return tableSize / days * 365, dynamicProperties, ""
	}
	// partition by year
	setDProps("YEAR")
	return tableSize / days * 365, dynamicProperties, fmt.Sprintf("  START (\"%s\") END (\"%s\") EVERY (INTERVAL 1 year)", tableCreatedTime.Format(common.DATE_TEMPLATE)[:4]+"-01-01", time.Now().AddDate(1, 0, 0).Format(common.DATE_TEMPLATE)[:4]+"-01-01")
}

func (c *StarRocks) calculateBuckets(partitionSize int64) int64 {
	if partitionSize < common.GIGA_BYTES {
		return 1
	}
	if partitionSize < c.config.BENum*common.GIGA_BYTES {
		return int64(math.Ceil(float64(partitionSize) / float64(common.GIGA_BYTES)))
	}
	return int64(math.Ceil(float64(partitionSize)/float64(common.GIGA_BYTES)/float64(c.config.BENum))) * c.config.BENum
}
