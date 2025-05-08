package source

import (
	"errors"
	"fmt"
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/model"
	"strings"

	"github.com/thoas/go-funk"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

type ClickHouseSource struct {
	DBSource
}

func (c *ClickHouseSource) Construct(config *conf.Config) IDBSource {
	c.config = config
	for _, tableRule := range c.config.TableRules {
		tableRule.SchemaPattern = ".*"
	}
	return c
}

func (c *ClickHouseSource) Databases() ([]string, error) {
	results := []map[string]interface{}{}
	c.db.Raw("SELECT name FROM system.databases where name not in ('information_schema', 'INFORMATION_SCHEMA', 'system')").Find(&results)
	return funk.Map(results, func(row map[string]interface{}) string {
		return row["name"].(string)
	}).([]string), nil
}

func (c *ClickHouseSource) Schemas(db string) ([]string, error) {
	return []string{}, nil
}

func (c *ClickHouseSource) Tables(db, _ string) ([]string, error) {
	results := []map[string]interface{}{}
	c.db.Raw(fmt.Sprintf("SELECT name FROM system.tables WHERE database='%s' and is_temporary = 0 and (engine like '%%MergeTree' or engine like '%%Log') and name not like '.inner%%'", db)).Find(&results)
	return funk.Map(results, func(row map[string]interface{}) string {
		return row["name"].(string)
	}).([]string), nil
}

func (c *ClickHouseSource) Sample(db, _, table string, limit int) ([]map[string]interface{}, error) {
	results := []map[string]interface{}{}
	c.db.Raw(fmt.Sprintf("SELECT * FROM `%s`.`%s` limit 0,%d;", db, table, limit)).Find(&results)
	return results, nil
}

func (c *ClickHouseSource) Destroy() {
	if c.db == nil {
		return
	}
	chDB, err := c.db.DB()
	if err != nil {
		return
	}
	chDB.Close()
}

func (c *ClickHouseSource) ResultConventers() int {
	return common.ConvertToStarRocks
}

func (c *ClickHouseSource) InitDB() error {
	// initialize database connection
	connectionStr := fmt.Sprintf("tcp://%s:%d?database=information_schema&username=%s&password=%s&read_timeout=10&write_timeout=10", c.config.DBHost, c.config.DBPort, c.config.DBUser, c.config.DBPassword)
	db, err := gorm.Open(clickhouse.Open(connectionStr), &gorm.Config{})
	if err != nil {
		return err
	}
	chDB, err := db.DB()
	if err != nil {
		return err
	}
	chDB.SetMaxIdleConns(2)
	chDB.SetMaxOpenConns(3)
	c.db = db
	return nil
}

func (c *ClickHouseSource) Build() (IDBSourceProvider, error) {
	matchedTables := []*model.Table{}
	c.db.Raw(`select a.database as table_catalog, a.database as table_schema, a.table as table_name,
	a.total_rows as table_rows, a.engine as engine, a.comment as table_comment, b.*
from system.tables a
left join (
	select table, database, sum(primary_key_bytes_in_memory) as index_length, sum(bytes_on_disk) as data_length, min(min_date) as create_time from system.parts where active and database not in ('information_schema', 'INFORMATION_SCHEMA', 'system') group by database, table 
) b
on a.table = b.table and a.database = b.database
where a.database not in ('information_schema', 'INFORMATION_SCHEMA', 'system') and a.is_temporary = 0 and (engine like '%MergeTree' or engine like '%Log')
order by a.database asc, a.table asc;`).Find(&matchedTables)
	if len(matchedTables) == 0 {
		return c, errors.New("Failed to get rows from information_schema.tables.")
	}
	matchedTables = funk.Filter(matchedTables, func(table *model.Table) bool {
		return funk.ContainsString([]string{"SummingMergeTree", "MergeTree", "ReplacingMergeTree"}, table.ENGINE) || strings.HasSuffix(table.ENGINE, "Log")
	}).([]*model.Table)
	// for _, table := range matchedTables {
	// 	if !funk.ContainsString([]string{"SummingMergeTree", "MergeTree", "ReplacingMergeTree"}, table.ENGINE) && !strings.HasSuffix(table.ENGINE, "Log") {
	// 		return c, errors.New(fmt.Sprintf("Engine[%s] not supported.", table.ENGINE))
	// 	}
	// }
	mvTables := []*model.Table{}
	c.db.Raw(`select uuid, database as table_catalog, database as table_schema, table as table_name, comment as table_comment from system.tables where engine = 'MaterializedView';`).Find(&mvTables)
	mvTableMap := map[string]*model.Table{}
	for _, mvTbl := range mvTables {
		mvTableMap[fmt.Sprintf(".inner_id.%s", mvTbl.UUID)] = mvTbl
	}
	for _, tbl := range matchedTables {
		if strings.HasPrefix(tbl.TABLE_NAME, ".inner_id.") {
			if _, ok := mvTableMap[tbl.TABLE_NAME]; !ok {
				return c, errors.New("Name of table[" + tbl.TABLE_NAME + "] not found")
			}
			if len(mvTableMap[tbl.TABLE_NAME].TABLE_COMMENT) > 0 {
				tbl.TABLE_COMMENT = mvTableMap[tbl.TABLE_NAME].TABLE_COMMENT
			}
			tbl.TABLE_NAME = mvTableMap[tbl.TABLE_NAME].TABLE_NAME
		}
	}

	allColumns := []*model.Column{}
	c.db.Raw("select * from information_schema.columns a left join system.columns b on a.table_schema = b.database and a.table_name = b.table and a.column_name = b.name where a.table_schema not in ('information_schema', 'INFORMATION_SCHEMA', 'system') order by a.ORDINAL_POSITION asc").Find(&allColumns)
	if len(allColumns) == 0 {
		return c, errors.New("Failed to get rows from information_schema.columns.")
	}
	for _, col := range allColumns {
		if strings.HasPrefix(col.TABLE_NAME, ".inner_id.") {
			if _, ok := mvTableMap[col.TABLE_NAME]; !ok {
				return c, errors.New("Name of table[" + col.TABLE_NAME + "] not found")
			}
			col.TABLE_NAME = mvTableMap[col.TABLE_NAME].TABLE_NAME
		}
	}
	keyColumnUsageRows := []*model.KeyColumnUsage{}
	for _, col := range allColumns {
		if col.IsInSortingKey {
			keyColumnUsageRows = append(keyColumnUsageRows, &model.KeyColumnUsage{
				ModelBase:        col.ModelBase,
				COLUMN_NAME:      col.COLUMN_NAME,
				ORDINAL_POSITION: col.ORDINAL_POSITION,
				CONSTRAINT_NAME:  "PRIMARY",
			})
		}
	}
	c.calculateRuledTablesMap(matchedTables, allColumns, keyColumnUsageRows)
	if len(c.ruledTablesMap) == 0 {
		return c, errors.New("No matching table columns found.")
	}
	return c, nil
}

func (c *ClickHouseSource) GetRuledTablesMap() map[*conf.TableRule][]*common.TableColumns {
	return c.ruledTablesMap
}

func (c *ClickHouseSource) FormatFlinkColumnDef(table *model.Table, column *model.Column) (string, error) {
	return "", nil
}

func (c *ClickHouseSource) FormatStarRocksColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := ""
	if strings.Contains(column.DATA_TYPE, "AggregateFunction(") {
		return "", errors.New("Columns with `AggregateFunction` are not supported.")
	}
	if strings.Contains(column.DATA_TYPE, "Nullable(") {
		column.DATA_TYPE = strings.Replace(column.DATA_TYPE, "Nullable(", "", -1)
		column.DATA_TYPE = strings.Replace(column.DATA_TYPE, ")", "", 1)
	}
	if strings.Contains(column.DATA_TYPE, "LowCardinality(") {
		column.DATA_TYPE = strings.Replace(column.DATA_TYPE, "LowCardinality(", "", -1)
		column.DATA_TYPE = strings.Replace(column.DATA_TYPE, ")", "", 1)
	}
	if strings.Contains(column.DATA_TYPE, "Decimal(") {
		column.DATA_TYPE = "Decimal"
	}
	colDataType = c.transType(column)
	if len(colDataType) == 0 {
		colDataType = "STRING"
	}

	nullableStr := "NULL"
	if column.IsInSortingKey || column.IS_NULLABLE == "1" {
		nullableStr = "NOT NULL"
	}
	defaultStr := ""
	if column.COLUMN_DEFAULT != nil {
		columnDefault := *column.COLUMN_DEFAULT
		if len(columnDefault) > 0 && !strings.HasPrefix(columnDefault, "map(") && !strings.HasPrefix(columnDefault, "now(") {
			if !strings.HasPrefix(columnDefault, "'") {
				columnDefault = fmt.Sprintf("'%s'", strings.Replace(columnDefault, "'", "\\'", -1))
			} else {
				columnDefault = fmt.Sprintf("%s", columnDefault)
			}
		}
		defaultStr = fmt.Sprintf("DEFAULT %s", columnDefault)
	}
	sumAggregation := ""
	if table.ENGINE == "SummingMergeTree" && !column.IsInSortingKey {
		sumAggregation = "SUM"
	}
	columnStr := fmt.Sprintf("  `%s` %s %s %s %s COMMENT \"%s\"", column.COLUMN_NAME, colDataType, sumAggregation, nullableStr, defaultStr, c.encodeComment(column.COLUMN_COMMENT))
	return columnStr, nil
}

func (c *ClickHouseSource) transType(column *model.Column) string {
	switch column.DATA_TYPE {
	case "Int8":
		return "TINYINT"
	case "UInt8", "Int16":
		return "SMALLINT"
	case "UInt16", "Int32":
		return "INT"
	case "UInt32", "Int64":
		return "BIGINT"
	case "UInt64":
		return "LARGEINT"
	case "Float", "Float32":
		return "FLOAT"
	case "Float64", "Double":
		return "DOUBLE"
	case "Decimal":
		if !c.config.UseDecimalV3 {
			if column.NUMERIC_PRECISION > 27 {
				column.NUMERIC_PRECISION = 27
			}
			if column.NUMERIC_SCALE <= 0 {
				column.NUMERIC_SCALE = 0
			}
			if column.NUMERIC_SCALE > column.NUMERIC_PRECISION {
				column.NUMERIC_SCALE = column.NUMERIC_PRECISION - 1
			}
		} else {
			if column.NUMERIC_PRECISION > 38 {
				column.NUMERIC_PRECISION = 38
			}
			if column.NUMERIC_SCALE > column.NUMERIC_PRECISION {
				column.NUMERIC_SCALE = column.NUMERIC_PRECISION - 1
			}
		}
		return fmt.Sprintf("DECIMAL(%d, %d)", column.NUMERIC_PRECISION, column.NUMERIC_SCALE)
	case "Date", "Date32":
		return "DATE"
	case "DateTime", "DateTime64", "Timestamp":
		return "DATETIME"
	}
	if strings.HasPrefix(column.DATA_TYPE, "Array(") {
		column.DATA_TYPE = strings.Replace(column.DATA_TYPE, "Array(", "", 1)
		column.DATA_TYPE = strings.Replace(column.DATA_TYPE, ")", "", 1)
		colDataType := c.transType(column)
		if len(colDataType) > 0 {
			colDataType = fmt.Sprintf("ARRAY<%s>", colDataType)
		} else {
			colDataType = "ARRAY<STRING>"
		}
		return colDataType
	}
	return ""
}

func (c *ClickHouseSource) GetFlinkConnectorName() string {
	return "jdbc"
}

func (c *ClickHouseSource) GetFlinkSpecialProps(matchedTableRule *conf.TableRule) map[string]string {
	return nil
}

func (c *ClickHouseSource) CombineSchemaName() bool {
	return false
}

// CREATE TABLE chtest.t5 \
// ( \
//     `k1` UInt16 default 1, \
//     `k2` String default 'a', \
//     `k3` Date default '2021-01-01', \
//     `k4` Date32 default now(), \
//     `k5` DateTime default now(), \
//     `k6` DateTime64(3, 'Europe/Moscow') default '2021-01-01 00:00:02', \
//     `k7` UUID, \
//     `k8` UInt32, \
//     `k9` UInt64, \
//     `k10` Int16, \
//     `k11` Int32, \
//     `k12` Int64, \
//     `k13` LowCardinality(String) default 'aaaa', \
//     `k14` IPv4, \
//     `k15` CHAR VARYING, \
//     `k16` NATIONAL CHAR, \
//     `k17` DOUBLE PRECISION, \
//     `k18` Point default '(0, 0)', \
//     `k19` Ring default '[(0, 0)]', \
//     `k20` Polygon default '[[(0, 0)]]', \
//     `k21` MultiPolygon default '[[[(0, 0)]],[[(0, 0)]]]', \
//     `k22` Map(String, UInt64) default map("k1", 1), \
//     `k25` Decimal32(3) default 3.2, \
//     `k26` Decimal64(3) default 3.3, \
//     `k27` Decimal128(3) default 3.4, \
//     `k28` Enum16('hello' = 1, 'world' = 2) default 'hello', \
//     `k29` Float32, \
//     `k30` Float64, \
//     `k31` UInt8, \
//     `k32` Int8, \
//     `k33` Nullable(Int8), \
//     `k34` FixedString(5), \
//     `k35` Array(Array(Nullable(UInt8))) default '[[1,2]]', \
//     `k36` Nested(id UInt8, id2 String) \
// ) \
// ENGINE = MergeTree \
// PARTITION BY k3 \
// PRIMARY KEY (k1,k2) \
// ORDER BY (k1,k2) \
// SAMPLE BY k1 \
// SETTINGS index_granularity = 8192;
