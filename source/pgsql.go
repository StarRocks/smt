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
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type PostgreSQLSource struct {
	DBSource
}

func (c *PostgreSQLSource) Construct(config *conf.Config) IDBSource {
	c.config = config
	return c
}

func (c *PostgreSQLSource) DescribeTable(db, schema, table string) ([]*model.Column, []*model.KeyColumnUsage, error) {
	return nil, nil, nil
}

func (c *PostgreSQLSource) Databases() ([]string, error) {
	databases := []string{}
	c.db.Table("pg_database").Select("datname").Where("datistemplate=false").Pluck("datname", &databases)
	return databases, nil
}

func (c *PostgreSQLSource) Schemas(db string) ([]string, error) {
	tables := []*model.Table{}
	c.SwitchDB(db)
	c.db.Select("table_schema").Where("table_type=? and table_catalog = ? and table_schema not in ('information_schema', 'pg_catalog')", "BASE TABLE", db).Order("TABLE_SCHEMA asc, TABLE_NAME asc").Find(&tables)
	return funk.Map(tables, func(table *model.Table) string {
		return table.TABLE_SCHEMA
	}).([]string), nil
}

func (c *PostgreSQLSource) Tables(db, schema string) ([]string, error) {
	tables := []*model.Table{}
	c.SwitchDB(db)
	c.db.Select("table_name").Where("table_type=? and table_catalog = ? and table_schema = ?", "BASE TABLE", db, schema).Order("TABLE_SCHEMA asc, TABLE_NAME asc").Find(&tables)
	return funk.Map(tables, func(table *model.Table) string {
		return table.TABLE_NAME
	}).([]string), nil
}

func (c *PostgreSQLSource) Sample(db, schema, table string, limit int) ([]map[string]interface{}, error) {
	results := []map[string]interface{}{}
	c.SwitchDB(db)
	c.db.Raw(fmt.Sprintf("SELECT * FROM %s.%s limit %d;", schema, table, limit)).Find(&results)
	return results, nil
}

func (c *PostgreSQLSource) Destroy() {
	if c.db == nil {
		return
	}
	pgSQLDB, err := c.db.DB()
	if err != nil {
		return
	}
	pgSQLDB.Close()
}

func (c *PostgreSQLSource) ResultConventers() int {
	return common.ConvertToStarRocks | common.ConvertToFlink
}

func (c *PostgreSQLSource) InitDB() error {
	return c.SwitchDB("")
}

func (c *PostgreSQLSource) SwitchDB(database string) error {
	if c.db != nil {
		pgSQL, err := c.db.DB()
		if err == nil {
			pgSQL.Close()
		}
		c.db = nil
	}
	connectionStr := fmt.Sprintf("host=%s user=%s password=%s port=%d sslmode=disable", c.config.DBHost, c.config.DBUser, c.config.DBPassword, c.config.DBPort)
	if len(database) > 0 {
		connectionStr = connectionStr + " dbname=" + database
	}
	db, err := gorm.Open(postgres.Open(connectionStr), &gorm.Config{})
	// initialize database connection
	if err != nil {
		return err
	}
	pgSQL, err := db.DB()
	if err != nil {
		return err
	}
	pgSQL.SetMaxIdleConns(2)
	pgSQL.SetMaxOpenConns(3)
	c.db = db
	return nil
}

func (c *PostgreSQLSource) Build() (IDBSourceProvider, error) {
	matchedTables := []*model.Table{}
	allColumns := []*model.Column{}
	keyColumnUsageRows := []*model.KeyColumnUsage{}
	databases := []string{}
	c.db.Table("pg_database").Select("datname").Where("datistemplate=false").Pluck("datname", &databases)
	if len(databases) == 0 {
		return c, errors.New("Failed to get databases from pgsql.")
	}
	databases = funk.Filter(databases, func(database string) bool {
		return funk.Some(c.config.TableRules, func(tableRule *conf.TableRule) bool {
			return common.RegMatchString(tableRule.DatabasePattern, database)
		})
	}).([]string)
	for _, database := range databases {
		tables := []*model.Table{}
		c.SwitchDB(database)
		err := c.db.Select("table_name", "table_schema", "table_catalog", "pg_table_size(table_schema || '.' || table_name) as data_length", "pg_indexes_size(table_schema || '.' || table_name) as index_length").Where("table_type=? and table_schema not in ('information_schema', 'pg_catalog')", "BASE TABLE").Order("TABLE_SCHEMA asc, TABLE_NAME asc").Find(&tables).Error
		if err != nil {
			return c, err
		}
		tables = funk.Filter(tables, func(table *model.Table) bool {
			table.CREATE_TIME = time.Now().Add(-24 * 365 * time.Hour)
			return funk.Some(c.config.TableRules, func(tableRule *conf.TableRule) bool {
				return common.RegMatchString(tableRule.DatabasePattern, table.TABLE_CATALOG) &&
					common.RegMatchString(tableRule.SchemaPattern, table.TABLE_SCHEMA) &&
					common.RegMatchString(tableRule.TablePattern, table.TABLE_NAME)
			})
		}).([]*model.Table)
		matchedTables = append(matchedTables, tables...)
		columns := []*model.Column{}
		tableNames := funk.Map(tables, func(table *model.Table) string {
			return table.TABLE_NAME
		}).([]string)
		schemaNames := funk.Map(tables, func(table *model.Table) string {
			return table.TABLE_SCHEMA
		}).([]string)
		err = c.db.Where("table_catalog = ? and table_name in ? and table_schema in ?", database, tableNames, schemaNames).Order("ORDINAL_POSITION asc").Find(&columns).Error
		if err != nil {
			return c, err
		}
		allColumns = append(allColumns, columns...)
		keyColumns := []*model.KeyColumnUsage{}
		err = c.db.Where("table_catalog = ? and table_name in ? and table_schema in ?", database, tableNames, schemaNames).Find(&keyColumns).Error
		if err != nil {
			return c, err
		}
		keyColumnUsageRows = append(keyColumnUsageRows, keyColumns...)
	}
	if len(matchedTables) == 0 {
		return c, errors.New("Failed to get rows from information_schema.tables.")
	}
	if len(allColumns) == 0 {
		return c, errors.New("Failed to get rows from information_schema.columns.")
	}
	for _, keyColumn := range keyColumnUsageRows {
		if strings.HasSuffix(keyColumn.CONSTRAINT_NAME, "_pkey") {
			keyColumn.CONSTRAINT_NAME = "PRIMARY"
		}
	}
	c.calculateRuledTablesMap(matchedTables, allColumns, keyColumnUsageRows)
	if len(c.ruledTablesMap) == 0 {
		return c, errors.New("No matching table columns found.")
	}
	return c, nil
}

func (c *PostgreSQLSource) GetRuledTablesMap() map[*conf.TableRule][]*common.TableColumns {
	return c.ruledTablesMap
}

func (c *PostgreSQLSource) FormatFlinkColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := c.convertType(column.DATA_TYPE, column.UDT_NAME, column.NUMERIC_PRECISION, column.NUMERIC_SCALE, true)
	nullableStr := "NULL"
	if column.IS_NULLABLE != "YES" {
		nullableStr = "NOT NULL"
	}
	columnStr := fmt.Sprintf("  `%s` %s %s", column.COLUMN_NAME, colDataType, nullableStr)
	return columnStr, nil
}

func (c *PostgreSQLSource) FormatStarRocksColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := c.convertType(column.DATA_TYPE, column.UDT_NAME, column.NUMERIC_PRECISION, column.NUMERIC_SCALE, false)
	nullableStr := "NULL"
	if column.IS_NULLABLE != "YES" {
		nullableStr = "NOT NULL"
	}
	defaultStr := ""
	colDataType = strings.Replace(colDataType, "unsigned", "", -1)
	colDataType = strings.Replace(colDataType, "UNSIGNED", "", -1)
	columnStr := fmt.Sprintf("  `%s` %s %s %s COMMENT \"%s\"", column.COLUMN_NAME, colDataType, nullableStr, defaultStr, c.encodeComment(column.COLUMN_COMMENT))
	return columnStr, nil
}

func (c *PostgreSQLSource) convertType(dt, udt string, NUMERIC_PRECISION, NUMERIC_SCALE uint64, flink bool) string {
	switch strings.ReplaceAll(strings.ToLower(dt), "_", "") {
	case "char", "character", "character varying", "varchar", "text", "json", "bytea", "uuid":
		return "STRING"
	case "smallint", "smallserial", "int2":
		return "SMALLINT"
	case "integer", "serial", "int4":
		return "INT"
	case "bigint", "bigserial", "int8":
		return "BIGINT"
	case "decimal", "numeric":
		if !c.config.UseDecimalV3 {
			if NUMERIC_PRECISION <= 0 {
				NUMERIC_PRECISION = 27
			}
			if NUMERIC_PRECISION > 27 {
				NUMERIC_PRECISION = 27
			}
			if NUMERIC_SCALE <= 0 {
				NUMERIC_SCALE = 0
			}
			if NUMERIC_SCALE > NUMERIC_PRECISION {
				NUMERIC_SCALE = NUMERIC_PRECISION - 1
			}
		} else {
			if NUMERIC_PRECISION <= 0 {
				NUMERIC_PRECISION = 38
			}
			if NUMERIC_PRECISION > 38 {
				NUMERIC_PRECISION = 38
			}
			if NUMERIC_SCALE > NUMERIC_PRECISION {
				NUMERIC_SCALE = NUMERIC_PRECISION - 1
			}
		}
		return fmt.Sprintf("DECIMAL(%d, %d)", NUMERIC_PRECISION, NUMERIC_SCALE)
	case "money":
		return fmt.Sprintf("DECIMAL(%d, %d)", 27, 2)
	case "date":
		return "DATE"
	case "timestamp", "timestamp without time zone", "timestamp time zone":
		if flink {
			return "TIMESTAMP"
		}
		return "DATETIME"
	case "boolean":
		return "BOOLEAN"
	case "bit", "bit varying":
		if NUMERIC_PRECISION < 8 {
			return "TINYINT"
		} else if NUMERIC_PRECISION < 16 {
			return "SMALLINT"
		} else if NUMERIC_PRECISION < 32 {
			return "INT"
		} else if NUMERIC_PRECISION < 64 {
			return "BIGINT"
		} else {
			return "LARGEINT"
		}
	case "float", "float4", "real":
		return "FLOAT"
	case "double", "double precision", "float8":
		return "DOUBLE"
	case "array":
		return "STRING"
		// return fmt.Sprintf("ARRAY<%s>", c.convertType(udt, udt, NUMERIC_PRECISION, NUMERIC_SCALE, flink))
	}
	return "STRING"
}

func (c *PostgreSQLSource) GetFlinkConnectorName() string {
	return "postgres-cdc"
}

func (c *PostgreSQLSource) GetFlinkSpecialProps(matchedTableRule *conf.TableRule) map[string]string {
	return map[string]string{
		"decoding.plugin.name": "pgoutput",
	}
}

func (c *PostgreSQLSource) CombineSchemaName() bool {
	return true
}

// ALTER TABLE public.pgtest REPLICA IDENTITY FULL
// create table sctest.pgtest(
// k1 int,
// k2 numeric,
// k3 float,
// k4 date,
// k5 timestamp,
// k6 timestamp[],
// k7 decimal(24, 5),
// k8 varchar(256),
// k9 json,
// primary key(k1)
// );
// insert into pgtest values(3, 2, 3, '2021-09-10', '2021-09-10 01:02:03.001', ARRAY[to_timestamp('2021-09-10 01:02:03', 'yyyy-MM-dd hh24:mi:ss')], 8.1, 'aaaccc', '{"a": "c"}'::json);

// echo "host all all 0.0.0.0/32 trust" >> pg_hba.conf
// echo "host replication all 0.0.0.0/32 trust" >> pg_hba.conf
// echo "wal_level = logical" >> postgresql.conf
// echo "max_wal_senders = 2" >> postgresql.conf
// echo "max_replication_slots = 8" >> postgresql.conf
