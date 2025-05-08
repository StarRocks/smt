package source

import (
	"fmt"
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/model"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/thoas/go-funk"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MySQLSource struct {
	DBSource
}

func (c *MySQLSource) Construct(config *conf.Config) IDBSource {
	c.config = config
	for _, tableRule := range c.config.TableRules {
		tableRule.SchemaPattern = ".*"
	}
	return c
}

func (c *MySQLSource) DescribeTable(db, schema, table string) ([]*model.Column, []*model.KeyColumnUsage, error) {
	allColumns := []*model.Column{}
	c.db.Where(&model.Column{
		ModelBase: model.ModelBase{
			TABLE_SCHEMA: db,
			TABLE_NAME:   table,
		},
	}).Order("ORDINAL_POSITION asc").Find(&allColumns)
	keyColumnUsageRows := []*model.KeyColumnUsage{}
	c.db.Where(&model.KeyColumnUsage{
		ModelBase: model.ModelBase{
			TABLE_SCHEMA: db,
			TABLE_NAME:   table,
		},
	}).Model(&model.KeyColumnUsage{}).Find(&keyColumnUsageRows)
	return allColumns, keyColumnUsageRows, nil
}

func (c *MySQLSource) Databases() ([]string, error) {
	results := []map[string]interface{}{}
	c.db.Raw("SHOW DATABASES").Find(&results)
	return funk.Map(results, func(row map[string]interface{}) string {
		return row["Database"].(string)
	}).([]string), nil
}

func (c *MySQLSource) Schemas(db string) ([]string, error) {
	return []string{}, nil
}

func (c *MySQLSource) Tables(db, _ string) ([]string, error) {
	results := []map[string]interface{}{}
	c.db.Raw(fmt.Sprintf("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='%s'", db)).Find(&results)
	return funk.Map(results, func(row map[string]interface{}) string {
		return row["TABLE_NAME"].(string)
	}).([]string), nil
}

func (c *MySQLSource) Sample(db, _, table string, limit int) ([]map[string]interface{}, error) {
	results := []map[string]interface{}{}
	c.db.Raw(fmt.Sprintf("SELECT * FROM `%s`.`%s` limit %d;", db, table, limit)).Find(&results)
	return results, nil
}

func (c *MySQLSource) Destroy() {
	if c.db == nil {
		return
	}
	mySQLDB, err := c.db.DB()
	if err != nil {
		return
	}
	mySQLDB.Close()
}

func (c *MySQLSource) ResultConventers() int {
	return common.ConvertToStarRocks | common.ConvertToStarRocksExternal | common.ConvertToFlink
}

func (c *MySQLSource) InitDB() error {
	// initialize database connection
	connectionStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema?charset=utf8&parseTime=True&loc=Local", c.config.DBUser, c.config.DBPassword, c.config.DBHost, c.config.DBPort)
	db, err := gorm.Open(mysql.Open(connectionStr), &gorm.Config{})
	if err != nil {
		return err
	}
	mysqlDB, err := db.DB()
	if err != nil {
		return err
	}
	mysqlDB.SetMaxIdleConns(2)
	mysqlDB.SetMaxOpenConns(3)
	c.db = db
	return nil
}

func (c *MySQLSource) Build() (IDBSourceProvider, error) {
	matchedTables := []*model.Table{}
	err := c.db.Where("TABLE_TYPE=?", "BASE TABLE").Order("TABLE_SCHEMA asc, TABLE_NAME asc").Find(&matchedTables).Error
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to get rows from information_schema.tables.")
	}

	if len(matchedTables) == 0 {
		return c, errors.New("Failed to get rows from information_schema.tables.")
	}
	for _, table := range matchedTables {
		table.TABLE_CATALOG = table.TABLE_SCHEMA
	}
	allColumns := []*model.Column{}
	err = c.db.Order("ORDINAL_POSITION asc").Find(&allColumns).Error
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to get rows from information_schema.columns.")
	}

	if len(allColumns) == 0 {
		return c, errors.New("Failed to get rows from information_schema.columns.")
	}
	for _, column := range allColumns {
		column.TABLE_CATALOG = column.TABLE_SCHEMA
	}
	keyColumnUsageRows := []*model.KeyColumnUsage{}
	err = c.db.Model(&model.KeyColumnUsage{}).Find(&keyColumnUsageRows).Error
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to get rows from information_schema.key_column_usage.")
	}

	for _, keyColumn := range keyColumnUsageRows {
		keyColumn.TABLE_CATALOG = keyColumn.TABLE_SCHEMA
	}
	c.calculateRuledTablesMap(matchedTables, allColumns, keyColumnUsageRows)
	if len(c.ruledTablesMap) == 0 {
		return c, errors.New("No matching table columns found.")
	}
	return c, nil
}

func (c *MySQLSource) GetRuledTablesMap() map[*conf.TableRule][]*common.TableColumns {
	return c.ruledTablesMap
}

func (c *MySQLSource) FormatFlinkColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := "STRING"
	switch column.DATA_TYPE {
	case "char", "year", "varchar", "tinytext", "text", "mediumtext", "longtext", "tinyblob", "blob", "mediumblob", "longblob", "json":
		colDataType = "STRING"
		break
	case "tinyint":
		if isUnsigned(column.COLUMN_TYPE) {
			colDataType = "SMALLINT"
		} else {
			colDataType = "TINYINT"
		}
		break
	case "smallint":
		if isUnsigned(column.COLUMN_TYPE) {
			colDataType = "INT"
		} else {
			colDataType = "SMALLINT"
		}
		break
	case "mediumint", "int", "integer":
		if isUnsigned(column.COLUMN_TYPE) {
			colDataType = "BIGINT"
		} else {
			colDataType = "INT"
		}
		break
	case "bigint":
		if isUnsigned(column.COLUMN_TYPE) {
			colDataType = "DECIMAL(20, 0)"
		} else {
			colDataType = "BIGINT"
		}
		break
	case "bit":
		if column.NUMERIC_PRECISION == 1 {
			colDataType = "BOOLEAN"
		} else {
			colDataType = "BINARY"
		}
		break
	case "real", "float":
		colDataType = "FLOAT"
		break
	case "binary", "varbinary", "double", "date":
		colDataType = strings.ToUpper(column.DATA_TYPE)
		break
	case "decimal":
		if (!c.config.UseDecimalV3 && column.NUMERIC_PRECISION > 27) || column.NUMERIC_PRECISION > 38 {
			colDataType = "STRING"
		} else {
			colDataType = fmt.Sprintf("DECIMAL(%d, %d)", column.NUMERIC_PRECISION, column.NUMERIC_SCALE)
		}
		break
	case "time", "datetime", "timestamp":
		colDataType = "TIMESTAMP"
		break
	case "enum":
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

func (c *MySQLSource) FormatStarRocksColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := ""
	switch column.DATA_TYPE {
	case "char", "year", "varchar", "tinytext", "text", "mediumtext", "longtext", "tinyblob", "blob", "mediumblob", "longblob", "json", "binary", "varbinary", "set":
		colDataType = "STRING"
		break
	case "tinyint":
		dataType := "TINYINT"
		if isUnsigned(column.COLUMN_TYPE) {
			dataType = "SMALLINT"
		}

		colDataType = strings.Replace(column.COLUMN_TYPE, column.DATA_TYPE, dataType, -1)
		break
	case "smallint":
		dataType := "SMALLINT"
		if isUnsigned(column.COLUMN_TYPE) {
			dataType = "INT"
		}

		colDataType = strings.Replace(column.COLUMN_TYPE, column.DATA_TYPE, dataType, -1)
		break
	case "mediumint", "int", "integer":
		dataType := "INT"
		if isUnsigned(column.COLUMN_TYPE) {
			dataType = "BIGINT"
		}

		colDataType = strings.Replace(column.COLUMN_TYPE, column.DATA_TYPE, dataType, -1)
		break
	case "bigint":
		dataType := "BIGINT"
		if isUnsigned(column.COLUMN_TYPE) {
			dataType = "LARGEINT"
		}

		colDataType = strings.Replace(column.COLUMN_TYPE, column.DATA_TYPE, dataType, -1)
		break
	case "bit":
		if column.NUMERIC_PRECISION < 8 {
			colDataType = "TINYINT"
		} else if column.NUMERIC_PRECISION < 16 {
			colDataType = "SMALLINT"
		} else if column.NUMERIC_PRECISION < 32 {
			colDataType = "INT"
		} else if column.NUMERIC_PRECISION < 64 {
			colDataType = "BIGINT"
		} else {
			colDataType = "LARGEINT"
		}
		break
	case "float":
		colDataType = "FLOAT"
		break
	case "double":
		colDataType = "DOUBLE"
		break
	case "decimal":
		if (!c.config.UseDecimalV3 && column.NUMERIC_PRECISION > 27) || column.NUMERIC_PRECISION > 38 {
			colDataType = "STRING"
		} else {
			colDataType = fmt.Sprintf("DECIMAL(%d, %d)", column.NUMERIC_PRECISION, column.NUMERIC_SCALE)
		}
		break
	case "date":
		colDataType = "DATE"
		break
	case "time", "datetime", "timestamp":
		colDataType = "DATETIME"
		break
	case "enum":
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
			if column.DATA_TYPE == "bit" {
				bitStr := strings.Replace(strings.Replace(strings.Replace(strings.ToLower(columnDefault), "b'", "", -1), "'", "", -1), "0b", "", -1)
				intStr, _ := strconv.ParseInt(bitStr, 2, 64)
				defaultStr = fmt.Sprintf("DEFAULT \"%d\"", intStr)
			} else {
				dateTemplate := ""
				if column.DATA_TYPE == "date" {
					dateTemplate = common.DATE_TEMPLATE
				}
				if column.DATA_TYPE == "datetime" || column.DATA_TYPE == "timestamp" {
					dateTemplate = common.DATETIME_TEMPLATE
				}
				if len(dateTemplate) > 0 {
					dt, err := time.Parse(dateTemplate, columnDefault)
					if err != nil {
						nullableStr = "NULL"
					} else {
						columnDefault = dt.Format(dateTemplate)
						defaultStr = fmt.Sprintf("DEFAULT \"%s\"", columnDefault)
					}
				} else {
					defaultStr = fmt.Sprintf("DEFAULT \"%s\"", columnDefault)
				}
			}
		} else {
			defaultStr = fmt.Sprintf("DEFAULT \"%s\"", columnDefault)
		}
	}
	colDataType = strings.Replace(colDataType, "unsigned", "", -1)
	colDataType = strings.Replace(colDataType, "UNSIGNED", "", -1)
	columnStr := fmt.Sprintf("  `%s` %s %s %s COMMENT \"%s\"", column.COLUMN_NAME, colDataType, nullableStr, defaultStr, c.encodeComment(column.COLUMN_COMMENT))
	return columnStr, nil
}

func (c *MySQLSource) GetFlinkConnectorName() string {
	return "mysql-cdc"
}

func (c *MySQLSource) GetFlinkSpecialProps(matchedTableRule *conf.TableRule) map[string]string {
	return nil
}

func (c *MySQLSource) CombineSchemaName() bool {
	return false
}

func isUnsigned(colDataType string) bool {
	return strings.Contains(colDataType, "unsigned") || strings.Contains(colDataType, "UNSIGNED")
}

// CREATE TABLE `test2` (
// 	`id` int NOT NULL AUTO_INCREMENT,
// 	`name` varchar(255)  DEFAULT NULL,
// 	`enable` bit(2) NOT NULL DEFAULT b'01',
//  	`test` enum('a', 'b', 'c') null default 'c',
// 	PRIMARY KEY (`id`) USING BTREE
//   )
