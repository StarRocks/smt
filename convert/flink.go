package convert

import (
	"fmt"
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/source"
	"strconv"
	"strings"

	funk "github.com/thoas/go-funk"
)

type Flink struct {
	Converter
}

func (c *Flink) Construct(config *conf.Config, dbProvider source.IDBSourceProvider) IConverter {
	c.dbProvider = dbProvider
	c.config = config
	return c
}

func (c *Flink) ResultFilePrefix() string {
	return "flink-create"
}

func (c *Flink) ToCreateDDL() ([]string, map[string][]string, error) {
	ddlList := []string{}
	ruledDDLMap := map[string][]string{}
	catalog := "default_catalog"
	for matchedTableRule, tableColumnsList := range c.dbProvider.GetRuledTablesMap() {
		mysqlCDCServerId := int64(-1)
		if c.config.DBType == common.DBSourceMySQL {
			if _, ok := matchedTableRule.FlinkSourceProps["server-id"]; ok {
				mysqlCDCServerId, _ = strconv.ParseInt(matchedTableRule.FlinkSourceProps["server-id"], 10, 64)
			}
		}
		for _, tableColumns := range tableColumnsList {
			if _, ok := ruledDDLMap[matchedTableRule.Seq]; !ok {
				ruledDDLMap[matchedTableRule.Seq] = []string{}
			}
			databaseName := tableColumns.Table.TABLE_CATALOG
			ddl := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`.`%s`", catalog, databaseName)
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
			srcDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n", catalog, databaseName, shemaPrefixedTableName+"_src")
			sinkDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n", catalog, databaseName, shemaPrefixedTableName+"_sink")
			columnStrList := []string{}
			primaryKeys := []string{}
			if len(tableColumns.PrimaryKCU) > 0 {
				primaryKeys, tableColumns.Columns = c.reorderTableColumns(tableColumns.PrimaryKCU, tableColumns.Columns)
			} else if len(tableColumns.UniqueKCU) > 0 {
				// unique keys as primary keys
				primaryKeys, tableColumns.Columns = c.reorderTableColumns(tableColumns.UniqueKCU, tableColumns.Columns)
			}
			// 1. concat columns
			for _, column := range tableColumns.Columns {
				columnStr, err := c.dbProvider.FormatFlinkColumnDef(tableColumns.Table, column)
				if err != nil {
					return ddlList, ruledDDLMap, err
				}
				columnStrList = append(columnStrList, columnStr)
			}
			srcDDL += strings.Join(columnStrList, ",\n")
			sinkDDL += strings.Join(columnStrList, ",\n")

			// 2. concat keys
			keysList := ""
			if len(primaryKeys) > 0 {
				keysList = strings.Join(funk.Map(primaryKeys, func(key string) string {
					return fmt.Sprintf("`%s`", key)
				}).([]string), ", ")
				srcDDL += fmt.Sprintf(",\n  PRIMARY KEY(%s)\n NOT ENFORCED", keysList)
				sinkDDL += fmt.Sprintf(",\n  PRIMARY KEY(%s)\n NOT ENFORCED", keysList)
			}
			srcDDL += "\n) with (\n"
			sinkDDL += "\n) with (\n"
			// 3. build source properties
			sourceProps := matchedTableRule.FlinkSourceProps
			userSetKeys := funk.Keys(sourceProps).([]string)
			sourceProps["connector"] = c.dbProvider.GetFlinkConnectorName()
			if c.config.DBType != common.DBSourceTiDB {
				sourceProps["hostname"] = c.config.DBHost
				sourceProps["port"] = strconv.FormatInt(c.config.DBPort, 10)
				sourceProps["username"] = c.config.DBUser
				sourceProps["password"] = c.config.DBPassword
			}

			if mysqlCDCServerId > 0 {
				sourceProps["server-id"] = fmt.Sprintf("%d", mysqlCDCServerId)
				mysqlCDCServerId++
			}
			if matchedTableRule.FromShardingSrc {
				sourceProps["database-name"] = c.trimRegex(matchedTableRule.DatabasePattern)
				sourceProps["table-name"] = c.trimRegex(matchedTableRule.TablePattern)
				if c.dbProvider.CombineSchemaName() {
					sourceProps["schema-name"] = c.trimRegex(matchedTableRule.SchemaPattern)
				}
			} else {
				sourceProps["database-name"] = tableColumns.Table.TABLE_CATALOG
				sourceProps["table-name"] = tableColumns.Table.TABLE_NAME
				if c.dbProvider.CombineSchemaName() {
					sourceProps["schema-name"] = tableColumns.Table.TABLE_SCHEMA
				}
			}
			if c.dbProvider.GetFlinkSpecialProps(matchedTableRule) != nil {
				for k, v := range c.dbProvider.GetFlinkSpecialProps(matchedTableRule) {
					if funk.ContainsString(userSetKeys, k) {
						continue
					}
					sourceProps[k] = v
				}
			}
			sourcePropsArr := []string{}
			for k, v := range sourceProps {
				sourcePropsArr = append(sourcePropsArr, fmt.Sprintf("  '%s' = '%s'", k, v))
			}
			srcDDL = srcDDL + strings.Join(sourcePropsArr, ",\n") + "\n)"
			if c.config.DBType != common.DBSourceHive {
				ddlList = append(ddlList, srcDDL)
			}
			ruledDDLMap[matchedTableRule.Seq] = append(ruledDDLMap[matchedTableRule.Seq], srcDDL)
			// 4. build sink properties
			sinkProps := matchedTableRule.FlinkSinkProps
			sinkProps["connector"] = "starrocks"
			sinkProps["database-name"] = tableColumns.Table.TABLE_CATALOG
			sinkProps["table-name"] = shemaPrefixedTableName
			sinkPropsArr := []string{}
			for k, v := range sinkProps {
				sinkPropsArr = append(sinkPropsArr, fmt.Sprintf("  '%s' = '%s'", k, v))
			}
			sinkDDL = sinkDDL + strings.Join(sinkPropsArr, ",\n") + "\n)"
			ddlList = append(ddlList, sinkDDL)
			ruledDDLMap[matchedTableRule.Seq] = append(ruledDDLMap[matchedTableRule.Seq], sinkDDL)

			// 5. add insert into
			sinkTableName := shemaPrefixedTableName + "_sink"
			srcTableName := shemaPrefixedTableName + "_src"
			insertInto := fmt.Sprintf("INSERT INTO `%s`.`%s`.`%s` SELECT * FROM `%s`.`%s`.`%s`", catalog, tableColumns.Table.TABLE_CATALOG, sinkTableName, catalog, tableColumns.Table.TABLE_CATALOG, srcTableName)
			if c.config.DBType == common.DBSourceHive {
				insertInto = fmt.Sprintf("INSERT INTO `%s`.`%s`.`%s` SELECT * FROM `%s`.`%s`", catalog, tableColumns.Table.TABLE_CATALOG, sinkTableName, tableColumns.Table.TABLE_CATALOG, shemaPrefixedTableName)
			}
			ddlList = append(ddlList, insertInto)
			ruledDDLMap[matchedTableRule.Seq] = append(ruledDDLMap[matchedTableRule.Seq], insertInto)
		}
	}
	return ddlList, ruledDDLMap, nil
}

func (c *Flink) trimRegex(regex string) string {
	if strings.HasSuffix(regex, "$") {
		regex = regex[:len(regex)-1]
	}
	if strings.HasPrefix(regex, "^") {
		regex = regex[1:]
	}
	return regex
}
