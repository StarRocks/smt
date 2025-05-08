package source

import (
	"errors"
	"fmt"
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/model"
	"strings"
	"time"

	"github.com/thoas/go-funk"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

type SQLServerSource struct {
	DBSource
}

func (c *SQLServerSource) Construct(config *conf.Config) IDBSource {
	c.config = config
	return c
}

func (c *SQLServerSource) DescribeTable(db, schema, table string) ([]*model.Column, []*model.KeyColumnUsage, error) {
	allColumns := []*model.Column{}
	c.db.Where(&model.Column{
		ModelBase: model.ModelBase{
			TABLE_CATALOG: db,
			TABLE_SCHEMA:  schema,
			TABLE_NAME:    table,
		},
	}).Order("ORDINAL_POSITION asc").Find(&allColumns)
	keyColumnUsageRows := []*model.KeyColumnUsage{}
	c.db.Where(&model.KeyColumnUsage{
		ModelBase: model.ModelBase{
			TABLE_CATALOG: db,
			TABLE_SCHEMA:  schema,
			TABLE_NAME:    table,
		},
	}).Model(&model.KeyColumnUsage{}).Find(&keyColumnUsageRows)
	return allColumns, keyColumnUsageRows, nil
}

func (c *SQLServerSource) Databases() ([]string, error) {
	results := []map[string]interface{}{}
	c.db.Raw("SELECT name FROM master.sys.databases").Find(&results)
	return funk.Map(results, func(row map[string]interface{}) string {
		return row["name"].(string)
	}).([]string), nil
}

func (c *SQLServerSource) Schemas(db string) ([]string, error) {
	results := []map[string]interface{}{}
	c.switchDB(db)
	c.db.Raw(fmt.Sprintf("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME='%s'", db)).Find(&results)
	return funk.Map(results, func(row map[string]interface{}) string {
		return row["SCHEMA_NAME"].(string)
	}).([]string), nil
}

func (c *SQLServerSource) Tables(db, schema string) ([]string, error) {
	results := []map[string]interface{}{}
	c.switchDB(db)
	c.db.Raw(fmt.Sprintf("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='%s' AND TABLE_SCHEMA='%s'", db, schema)).Find(&results)
	return funk.Map(results, func(row map[string]interface{}) string {
		return row["TABLE_NAME"].(string)
	}).([]string), nil
}

func (c *SQLServerSource) Sample(db, schema, table string, limit int) ([]map[string]interface{}, error) {
	results := []map[string]interface{}{}
	c.switchDB(db)
	c.db.Raw(fmt.Sprintf("SELECT TOP %d * FROM %s.%s.%s;", limit, db, schema, table)).Find(&results)
	return results, nil
}

func (c *SQLServerSource) Destroy() {
	if c.db == nil {
		return
	}
	msSQLDB, err := c.db.DB()
	if err != nil {
		return
	}
	msSQLDB.Close()
	c.db = nil
}

func (c *SQLServerSource) ResultConventers() int {
	return common.ConvertToStarRocks | common.ConvertToFlink
}

func (c *SQLServerSource) InitDB() error {
	return c.switchDB("")
}

func (c *SQLServerSource) switchDB(database string) error {
	c.Destroy()
	connectionStr := fmt.Sprintf("sqlserver://%s:%s@%s:%d", c.config.DBUser, c.config.DBPassword, c.config.DBHost, c.config.DBPort)
	if len(database) > 0 {
		connectionStr += fmt.Sprintf("?database=%s", database)
	}

	db, err := gorm.Open(sqlserver.Open(connectionStr), &gorm.Config{})
	if err != nil {
		return err
	}

	ssDB, err := db.DB()
	if err != nil {
		return err
	}
	ssDB.SetMaxIdleConns(2)
	ssDB.SetMaxOpenConns(3)
	c.db = db
	return nil
}

func (c *SQLServerSource) Build() (IDBSourceProvider, error) {
	databases, err := c.Databases()
	if err != nil {
		return nil, err
	}

	if len(databases) == 0 {
		return c, errors.New("Failed to get databases from sqlserver.")
	}
	databases = funk.Filter(databases, func(database string) bool {
		return funk.Some(c.config.TableRules, func(tableRule *conf.TableRule) bool {
			return common.RegMatchString(tableRule.DatabasePattern, database)
		})
	}).([]string)

	allMatchedTables := []*model.Table{}
	allColumns := []*model.Column{}
	allKeyColumnUsageRows := []*model.KeyColumnUsage{}
	for _, db := range databases {
		c.switchDB(db)
		matchedTables := []*model.Table{}
		c.db.Where("TABLE_TYPE=?", "BASE TABLE").Order("TABLE_SCHEMA asc, TABLE_NAME asc").Find(&matchedTables)
		allMatchedTables = append(allMatchedTables, matchedTables...)
		columns := []*model.Column{}
		c.db.Order("ORDINAL_POSITION asc").Find(&columns)
		allColumns = append(allColumns, columns...)
		keyColumnUsageRows := []*model.KeyColumnUsage{}
		c.db.Model(&model.KeyColumnUsage{}).Find(&keyColumnUsageRows)
		allKeyColumnUsageRows = append(allKeyColumnUsageRows, keyColumnUsageRows...)
	}
	c.calculateRuledTablesMap(allMatchedTables, allColumns, allKeyColumnUsageRows)
	if len(c.ruledTablesMap) == 0 {
		return c, errors.New("No matching table columns found.")
	}
	return c, nil
}

func (c *SQLServerSource) GetRuledTablesMap() map[*conf.TableRule][]*common.TableColumns {
	return c.ruledTablesMap
}

func (c *SQLServerSource) FormatFlinkColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := ""
	switch column.DATA_TYPE {
	case "char", "varchar", "nvarchar", "nchar", "text", "ntext", "xml":
		colDataType = "STRING"
		break
	case "decimal", "money", "smallmoney":
		colDataType = fmt.Sprintf("DECIMAL(%d, %d)", column.NUMERIC_PRECISION, column.NUMERIC_SCALE)
		break
	case "numeric":
		colDataType = "NUMERIC"
		break
	case "float", "real":
		colDataType = "FLOAT"
		if column.NUMERIC_PRECISION > 24 {
			colDataType = "DOUBLE"
		}
		break
	case "bit":
		colDataType = "BOOLEAN"
		break
	case "tinyint", "smallint", "int", "bigint", "date":
		colDataType = strings.ToUpper(column.DATA_TYPE)
		break
	case "time", "datetime2", "datetime", "smalldatetime":
		colDataType = "TIMESTAMP"
		break
	case "datetimeoffset":
		colDataType = "TIMESTAMP_LTZ(3)"
		break
	default:
		colDataType = "STRING"
		break
	}

	nullableStr := "NULL"
	if column.IS_NULLABLE != "YES" {
		nullableStr = "NOT NULL"
	}
	columnStr := fmt.Sprintf("  `%s` %s %s", column.COLUMN_NAME, colDataType, nullableStr)
	return columnStr, nil
}

func (c *SQLServerSource) FormatStarRocksColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := ""
	switch column.DATA_TYPE {
	case "char", "nchar", "varchar", "nvarchar", "text", "ntext":
		colDataType = "STRING"
		break
	case "tinyint", "smallint", "int", "bigint":
		colDataType = strings.ToUpper(column.DATA_TYPE)
		break
	case "bit":
		colDataType = "BOOLEAN"
		break
	case "float", "real":
		colDataType = "FLOAT"
		if column.NUMERIC_PRECISION > 24 {
			colDataType = "DOUBLE"
		}
		break
	case "decimal", "numeric", "money", "smallmoney":
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
		colDataType = fmt.Sprintf("DECIMAL(%d, %d)", column.NUMERIC_PRECISION, column.NUMERIC_SCALE)
		break
	case "date":
		colDataType = "DATE"
		break
	case "time", "datetime", "datetime2", "smalldatetime", "datetimeoffset":
		colDataType = "DATETIME"
		break
	default:
		colDataType = "STRING"
		break
	}

	nullableStr := "NULL"
	if column.IS_NULLABLE != "YES" {
		nullableStr = "NOT NULL"
	}
	defaultStr := ""
	if column.COLUMN_DEFAULT != nil {
		columnDefault := *column.COLUMN_DEFAULT
		if len(columnDefault) > 0 {
			if strings.HasPrefix(columnDefault, "(") {
				columnDefault = columnDefault[1:]
			}
			if strings.HasSuffix(columnDefault, ")") {
				columnDefault = columnDefault[0 : len(columnDefault)-1]
			}
			if strings.HasPrefix(columnDefault, "'") {
				columnDefault = columnDefault[1:]
			}
			if strings.HasSuffix(columnDefault, "'") {
				columnDefault = columnDefault[0 : len(columnDefault)-1]
			}

			if columnDefault != "NULL" {
				dateTemplate := ""
				if column.DATA_TYPE == "date" {
					dateTemplate = common.DATE_TEMPLATE
				}
				if column.DATA_TYPE == "datetime" || column.DATA_TYPE == "datetime2" {
					dateTemplate = common.DATETIME_TEMPLATE
				}
				if column.DATA_TYPE == "time" {
					dateTemplate = common.TIME_TEMPLATE
				}
				if column.DATA_TYPE == "smalldatetime" {
					dateTemplate = common.SMALL_DATETIME_TEMPLATE
				}
				if column.DATA_TYPE == "datetimeoffset" {
					dateTemplate = common.DATETIME_ISO_TEMPLATE
				}
				if len(dateTemplate) > 0 {
					dt, err := time.Parse(dateTemplate, columnDefault)
					if err != nil {
						nullableStr = "NULL"
					} else {
						columnDefault = dt.Format(dateTemplate)
						if column.DATA_TYPE == "time" {
							columnDefault = "1970-01-01 " + columnDefault
						}
						if column.DATA_TYPE == "smalldatetime" {
							columnDefault = columnDefault + ":00"
						}
						defaultStr = fmt.Sprintf("DEFAULT \"%s\"", columnDefault)
					}
				} else {
					defaultStr = fmt.Sprintf("DEFAULT \"%s\"", columnDefault)
				}
			} else {
				nullableStr = "NULL"
			}
		} else {
			defaultStr = fmt.Sprintf("DEFAULT \"%s\"", columnDefault)
		}
	}
	columnStr := fmt.Sprintf("  `%s` %s %s %s COMMENT \"%s\"", column.COLUMN_NAME, colDataType, nullableStr, defaultStr, c.encodeComment(column.COLUMN_COMMENT))
	return columnStr, nil
}

func (c *SQLServerSource) GetFlinkConnectorName() string {
	return "sqlserver-cdc"
}

func (c *SQLServerSource) GetFlinkSpecialProps(matchedTableRule *conf.TableRule) map[string]string {
	return nil
}

func (c *SQLServerSource) CombineSchemaName() bool {
	return true
}

// func (c *SQLServerSource) TestResultConventers() {
// 	// initialize database connection
// 	c.db.Exec("DROP DATABASE testdb")
// 	c.db.Exec("CREATE DATABASE testdb")
// 	c.db.Exec("USE testdb")
// 	c.db.Exec("DROP SCHEMA testsch1")
// 	c.db.Exec("CREATE SCHEMA testsch1")
// 	c.db.Exec(`CREATE TABLE testsch1.test_table3 (
// id int NOT NULL,
// k1 tinyint DEFAULT NULL,
// k2 smallint DEFAULT NULL,
// k3 binary DEFAULT NULL,
// k4 bigint DEFAULT NULL,
// k5 decimal(10,5) DEFAULT NULL,
// k6 numeric(10,5) DEFAULT NULL,
// k7 time DEFAULT '00:00:01',
// k8 smallmoney DEFAULT '11.11',
// k9 money not null DEFAULT '11.12',
// k10 bit DEFAULT '1',
// k11 float(12) DEFAULT '1.22',
// k12 float(53) DEFAULT '2.11',
// k13 real DEFAULT '3.12',
// k14 date DEFAULT '2022-01-01',
// k15 datetime DEFAULT '2022-01-01 00:01:01',
// k16 datetime2 DEFAULT '2022-01-01 00:01:01.001',
// k17 smalldatetime DEFAULT '2022-01-01 00:00:01',
// k18 datetimeoffset DEFAULT '2022-01-01T00:00:01Z',
// k19 char DEFAULT 'a',
// k20 varchar(251) not null DEFAULT 'b',
// k21 text DEFAULT 'c',
// k22 nchar DEFAULT 'a',
// k23 nvarchar(251) not null DEFAULT 'b',
// k24 ntext DEFAULT 'c',
// PRIMARY KEY (id)
// )`)
// 	tables := []model.Column{}
// 	c.db.Model(&model.Column{}).Where("table_schema = ?", "testsch1").Find(&tables)
// 	return
// }
