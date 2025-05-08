package convert

import (
	"errors"
	"fmt"
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/source"
	"strings"

	funk "github.com/thoas/go-funk"
)

type StarRocksExternal struct {
	Converter
}

func (c *StarRocksExternal) Construct(config *conf.Config, dbProvider source.IDBSourceProvider) IConverter {
	c.dbProvider = dbProvider
	c.config = config
	return c
}

func (c *StarRocksExternal) ResultFilePrefix() string {
	return "starrocks-external-create"
}

func (c *StarRocksExternal) ToCreateDDL() ([]string, map[string][]string, error) {
	ddlList := []string{}
	ruledDDLMap := map[string][]string{}

	defaultHiveResourceName := "hive_external_resource"

	if c.config.DBType == common.DBSourceHive {
		dbProvider := c.dbProvider.(*source.HiveSource)
		metaURI, err := dbProvider.GetMetaStoreURI()
		if err != nil {
			return ddlList, ruledDDLMap, errors.New(err.Error())
		}
		metaPort := metaURI[strings.LastIndex(metaURI, ":")+1:]
		ddlList = append(ddlList, fmt.Sprintf("CREATE EXTERNAL RESOURCE \"%s\"\nPROPERTIES (\n  \"type\" = \"hive\",\n  \"hive.metastore.uris\" = \"thrift://%s:%s\"\n)", defaultHiveResourceName, c.config.DBHost, metaPort))
	}

	for matchedTableRule, tableColumnsList := range c.dbProvider.GetRuledTablesMap() {
		for _, tableColumns := range tableColumnsList {
			engine := ""
			switch c.config.DBType {
			case common.DBSourceMySQL, common.DBSourceTiDB:
				engine = "mysql"
				matchedTableRule.ExternalProperties["host"] = c.config.DBHost
				matchedTableRule.ExternalProperties["port"] = fmt.Sprintf("%d", c.config.DBPort)
				matchedTableRule.ExternalProperties["user"] = c.config.DBUser
				matchedTableRule.ExternalProperties["password"] = c.config.DBPassword
				matchedTableRule.ExternalProperties["database"] = tableColumns.Table.TABLE_CATALOG
				matchedTableRule.ExternalProperties["table"] = tableColumns.Table.TABLE_NAME
				break
			case common.DBSourceHive:
				engine = "hive"
				matchedTableRule.ExternalProperties["database"] = tableColumns.Table.TABLE_CATALOG
				matchedTableRule.ExternalProperties["table"] = tableColumns.Table.TABLE_NAME
				matchedTableRule.ExternalProperties["resource"] = defaultHiveResourceName
				break
			}
			partitionKey := ""
			if _, ok := ruledDDLMap[matchedTableRule.Seq]; !ok {
				ruledDDLMap[matchedTableRule.Seq] = []string{}
			}
			if len(matchedTableRule.PartitionKey) > 0 {
				partitionKey = matchedTableRule.PartitionKey
			}
			databaseName := fmt.Sprintf("%s_external_%s", engine, tableColumns.Table.TABLE_CATALOG)
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
			createTableDDL := fmt.Sprintf("CREATE EXTERNAL TABLE `%s`.`%s` (\n", databaseName, shemaPrefixedTableName)
			columnStrList := []string{}
			// unique keys as primary keys
			_, tableColumns.Columns = c.reorderTableColumns(tableColumns.UniqueKCU, tableColumns.Columns)
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
			createTableDDL += strings.Join(columnStrList, ",\n") + fmt.Sprintf("\n) ENGINE=%s\n", engine)

			// 2. concat comment
			createTableDDL += fmt.Sprintf("COMMENT \"%s\"\n", tableColumns.Table.TABLE_COMMENT)

			// 3. concat properties
			properties := matchedTableRule.ExternalProperties
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
