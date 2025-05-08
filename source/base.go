package source

import (
	"math"
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/model"
	"strings"
	"time"

	"gorm.io/gorm"
)

// IDBSource DBSource interface
type IDBSource interface {
	Construct(config *conf.Config) IDBSource
	InitDB() error
	Build() (IDBSourceProvider, error)
	Sample(db, schema, table string, limit int) ([]map[string]interface{}, error)
	// DescribeTable(db, schema, table string) ([]*model.Column, []*model.KeyColumnUsage, error)
	Databases() ([]string, error)
	Schemas(db string) ([]string, error)
	Tables(db, schema string) ([]string, error)
	Destroy()
}

type IDBSourceProvider interface {
	CombineSchemaName() bool
	GetFlinkConnectorName() string
	GetFlinkSpecialProps(matchedTableRule *conf.TableRule) map[string]string
	GetRuledTablesMap() map[*conf.TableRule][]*common.TableColumns
	FormatStarRocksColumnDef(table *model.Table, column *model.Column) (string, error)
	FormatFlinkColumnDef(table *model.Table, column *model.Column) (string, error)
	ResultConventers() int
}

// DBSource service struct
type DBSource struct {
	config         *conf.Config
	db             *gorm.DB
	ruledTablesMap map[*conf.TableRule][]*common.TableColumns
}

func Create(config *conf.Config) IDBSource {
	switch config.DBType {
	case common.DBSourceMySQL:
		return new(MySQLSource).Construct(config)
	case common.DBSourcePostgreSQL:
		return new(PostgreSQLSource).Construct(config)
	case common.DBSourceSQLServer:
		return new(SQLServerSource).Construct(config)
	case common.DBSourceOracle:
		return new(OracleSource).Construct(config)
	case common.DBSourceClickHouse:
		return new(ClickHouseSource).Construct(config)
	case common.DBSourceHive:
		return new(HiveSource).Construct(config)
	case common.DBSourceTiDB:
		return new(TiDBSource).Construct(config)
	}
	return nil
}

func (c *DBSource) calculateRuledTablesMap(matchedTables []*model.Table, allColumns []*model.Column, keyColumnUsageRows []*model.KeyColumnUsage) {
	c.ruledTablesMap = map[*conf.TableRule][]*common.TableColumns{}
	for _, table := range matchedTables {
		matchedTableRule := &conf.TableRule{}
		for _, tableRule := range c.config.TableRules {
			if common.RegMatchString(tableRule.DatabasePattern, table.TABLE_CATALOG) &&
				common.RegMatchString(tableRule.SchemaPattern, table.TABLE_SCHEMA) &&
				common.RegMatchString(tableRule.TablePattern, table.TABLE_NAME) {
				// last matched table rule
				matchedTableRule = tableRule
			}
		}
		if matchedTableRule.Properties == nil {
			continue
		}
		if _, ok := c.ruledTablesMap[matchedTableRule]; !ok {
			c.ruledTablesMap[matchedTableRule] = []*common.TableColumns{}
		}
		columns := []*model.Column{}
		for _, col := range allColumns {
			if col.TABLE_SCHEMA == table.TABLE_SCHEMA && col.TABLE_NAME == table.TABLE_NAME && col.TABLE_CATALOG == table.TABLE_CATALOG {
				columns = append(columns, col)
			}
		}
		primaryKCU := []*model.KeyColumnUsage{}
		uniqueKCU := []*model.KeyColumnUsage{}
		for _, keyCol := range keyColumnUsageRows {
			if keyCol.TABLE_SCHEMA != table.TABLE_SCHEMA || keyCol.TABLE_NAME != table.TABLE_NAME || keyCol.TABLE_CATALOG != table.TABLE_CATALOG {
				continue
			}
			if keyCol.CONSTRAINT_NAME == "PRIMARY" || strings.HasPrefix(keyCol.CONSTRAINT_NAME, "PK__") {
				primaryKCU = append(primaryKCU, keyCol)
			} else {
				uniqueKCU = append(uniqueKCU, keyCol)
			}
		}
		c.ruledTablesMap[matchedTableRule] = append(c.ruledTablesMap[matchedTableRule], &common.TableColumns{
			Table:      table,
			Columns:    columns,
			PrimaryKCU: primaryKCU,
			UniqueKCU:  uniqueKCU,
		})
	}
	for rule, tables := range c.ruledTablesMap {
		tmpColumns := []*model.Column{}
		if len(tables) == 1 {
			rule.FromShardingSrc = false
		}
		for _, table := range tables {
			columns := table.Columns
			if rule.FromShardingSrc {
				if len(tmpColumns) == 0 {
					tmpColumns = columns
				} else {
					if len(tmpColumns) != len(columns) {
						rule.FromShardingSrc = false
					} else {
						for idx, col := range columns {
							if tmpColumns[idx].COLUMN_NAME != col.COLUMN_NAME || tmpColumns[idx].COLUMN_TYPE != col.COLUMN_TYPE {
								rule.FromShardingSrc = false
							}
						}
					}
				}
			}
		}
	}
	for rule, tables := range c.ruledTablesMap {
		if rule.FromShardingSrc {
			totalSize := uint64(0)
			firstCreatedTime := time.Now().Unix()
			databaseNames := []string{}
			schemaNames := []string{}
			tableNames := []string{}
			for _, table := range tables {
				databaseNames = append(databaseNames, table.Table.TABLE_CATALOG)
				schemaNames = append(schemaNames, table.Table.TABLE_SCHEMA)
				tableNames = append(tableNames, table.Table.TABLE_NAME)
				totalSize += table.Table.DATA_LENGTH
				firstCreatedTime = int64(math.Min(float64(table.Table.CREATE_TIME.Unix()), float64(firstCreatedTime)))
			}
			singleTable := tables[0]
			singleTable.Table.CREATE_TIME = time.Unix(firstCreatedTime, 0)
			singleTable.Table.DATA_LENGTH = totalSize
			databaseName := common.LongestCommonXfix(databaseNames, true)
			if len(databaseName) == 0 {
				databaseName = "db" + common.SHARD_SUFFIX
			} else if databaseName == databaseNames[0] {
				databaseName = databaseNames[0]
			} else {
				databaseName = databaseName + common.SHARD_SUFFIX
			}
			schemaName := common.LongestCommonXfix(schemaNames, true)
			if len(schemaName) == 0 {
				schemaName = "schema" + common.SHARD_SUFFIX
			} else if schemaName == schemaNames[0] {
				schemaName = schemaNames[0]
			} else {
				schemaName = schemaName + common.SHARD_SUFFIX
			}
			tableName := common.LongestCommonXfix(tableNames, true)
			if len(tableName) == 0 {
				tableName = "table" + common.SHARD_SUFFIX
			} else if tableName == tableNames[0] {
				tableName = tableNames[0]
			} else {
				tableName = tableName + common.SHARD_SUFFIX
			}
			singleTable.Table.TABLE_CATALOG = databaseName
			singleTable.Table.TABLE_SCHEMA = schemaName
			singleTable.Table.TABLE_NAME = tableName
			for _, kcu := range singleTable.PrimaryKCU {
				kcu.TABLE_CATALOG = databaseName
				kcu.TABLE_SCHEMA = schemaName
				kcu.TABLE_NAME = tableName
			}
			for _, kcu := range singleTable.UniqueKCU {
				kcu.TABLE_CATALOG = databaseName
				kcu.TABLE_SCHEMA = schemaName
				kcu.TABLE_NAME = tableName
			}
			for _, col := range singleTable.Columns {
				col.TABLE_CATALOG = databaseName
				col.TABLE_SCHEMA = schemaName
				col.TABLE_NAME = tableName
			}
			c.ruledTablesMap[rule] = []*common.TableColumns{singleTable}
		}
	}
}

func (c *DBSource) encodeComment(comment string) string {
	comment = strings.Replace(comment, "\"", "\\\"", -1)
	comment = strings.Replace(comment, "\n", " ", -1)
	comment = strings.Replace(comment, "\r", " ", -1)
	return comment
}
