package source

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/model"
	"strings"

	_ "github.com/sijms/go-ora/v2"
	"github.com/thoas/go-funk"
)

type OracleSource struct {
	DBSource
	odb       *sql.DB
	dbVersion string
	cdbMode   bool
	cdbName   string
}

func (c *OracleSource) Construct(config *conf.Config) IDBSource {
	c.config = config
	return c
}

func (c *OracleSource) DescribeTable(db, schema, table string) ([]*model.Column, []*model.KeyColumnUsage, error) {
	return nil, nil, nil
}

func (c *OracleSource) Databases() ([]string, error) {
	databases := []string{}
	rows, err := c.odb.Query("select NAME from v$database")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			db := ""
			rows.Scan(&db)
			databases = append(databases, db)
		}
	}
	return databases, nil
}

func (c *OracleSource) Schemas(db string) ([]string, error) {
	schemas := []string{}
	rows, err := c.odb.Query("select sys_context( 'userenv', 'current_schema' ) from dual")
	if err != nil {
		return schemas, err
	}
	defer rows.Close()
	schemaName := ""
	for rows.Next() {
		rows.Scan(&schemaName)
		schemas = append(schemas, schemaName)
	}
	return schemas, nil
}

func (c *OracleSource) Tables(db, schema string) ([]string, error) {
	tables := []string{}
	rows, err := c.odb.Query(fmt.Sprintf("select table_name from all_tables where dropped='NO' and table_name != 'LOG_MINING_FLUSH' and owner = '%s'", schema))
	defer rows.Close()
	if err != nil {
		return tables, err
	}
	for rows.Next() {
		tableName := ""
		rows.Scan(&tableName)
		tables = append(tables, tableName)
	}
	return tables, nil
}

func (c *OracleSource) Sample(db, _, table string, limit int) ([]map[string]interface{}, error) {
	err := c.SwitchDB(db)
	if err != nil {
		return nil, err
	}
	rows, err := c.odb.Query(fmt.Sprintf("SELECT * FROM %s where rownum<=%d;", table, limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	columnLength := len(columns)
	cache := make([]interface{}, columnLength)
	for index := range cache {
		var a interface{}
		cache[index] = &a
	}
	var list []map[string]interface{}
	for rows.Next() {
		_ = rows.Scan(cache...)
		item := make(map[string]interface{})
		for i, data := range cache {
			item[columns[i]] = *data.(*interface{})
		}
		list = append(list, item)
	}
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (c *OracleSource) Destroy() {
	if c.odb == nil {
		return
	}
	c.odb.Close()
}

func (c *OracleSource) ResultConventers() int {
	return common.ConvertToStarRocks | common.ConvertToFlink
}

func (c *OracleSource) InitDB() error {
	err := c.SwitchDB(c.config.TableRules[0].DatabasePattern)
	if err != nil {
		return err
	}
	rows, err := c.odb.Query("select sys_context( 'userenv', 'current_schema' ) from dual")
	if err != nil {
		return err
	}
	defer rows.Close()
	schemaName := ""
	for rows.Next() {
		rows.Scan(&schemaName)
	}
	if schemaName != strings.ToUpper(c.config.DBUser) {
		return errors.New("Username is different with the schema_name.")
	}
	rows, err = c.odb.Query("select BANNER from v$version")
	if err != nil {
		return errors.New(err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		if len(c.dbVersion) > 0 {
			break
		}
		rows.Scan(&c.dbVersion)
	}
	rows1, err := c.odb.Query("select NAME, CDB from v$database")
	if err == nil {
		defer rows1.Close()
		for rows1.Next() {
			if c.cdbMode {
				break
			}
			cdb := "NO"
			cdbName := ""
			rows1.Scan(&cdbName, &cdb)
			if cdb == "YES" {
				c.cdbMode = true
				c.cdbName = cdbName
			}
		}
	}
	return nil
}

func (c *OracleSource) SwitchDB(database string) error {
	if c.odb != nil {
		c.odb.Close()
		c.odb = nil
	}
	connStr := fmt.Sprintf("oracle://%s:%s@%s:%d/%s", url.PathEscape(c.config.DBUser), url.PathEscape(c.config.DBPassword), c.config.DBHost, c.config.DBPort, database)
	conn, err := sql.Open("oracle", connStr)
	if err != nil {
		return err
	}
	rows, err := conn.Query("select BANNER from v$version")
	if err != nil {
		return err
	}
	defer rows.Close()
	c.odb = conn
	return nil
}

func (c *OracleSource) Build() (IDBSourceProvider, error) {
	matchedTables := []*model.Table{}
	allColumns := []*model.Column{}
	keyColumnUsageRows := []*model.KeyColumnUsage{}
	for _, tableRule := range c.config.TableRules {
		tables := []*model.Table{}
		c.SwitchDB(tableRule.DatabasePattern)
		rows, err := c.odb.Query("select a.owner as table_schema, a.table_name, a.avg_row_len * a.num_rows as data_length, b.comments from all_tables a left join all_tab_comments b on a.table_name = b.table_name where a.dropped='NO' and a.table_name != 'LOG_MINING_FLUSH' and a.owner not in ('APEX_030200', 'XDB', 'WMSYS', 'OE', 'SH', 'PM', 'IX', 'MDSYS', 'OUTLN', 'CTXSYS', 'OLAPSYS', 'FLOWS_FILES', 'OWBSYS', 'HR', 'EXFSYS', 'SCOTT', 'DBSNMP', 'ORDSYS', 'SYSMAN', 'APPQOSSYS', 'ORDDATA', 'SYS', 'SYSTEM')")
		if err != nil {
			return c, err
		}
		defer rows.Close()
		for rows.Next() {
			table := &model.Table{}
			rows.Scan(&table.TABLE_SCHEMA, &table.TABLE_NAME, &table.DATA_LENGTH, &table.TABLE_COMMENT)
			if !common.RegMatchString(tableRule.TablePattern, table.TABLE_NAME) || !common.RegMatchString(tableRule.SchemaPattern, table.TABLE_SCHEMA) {
				continue
			}
			table.TABLE_CATALOG = tableRule.DatabasePattern
			tables = append(tables, table)
		}
		if len(tables) == 0 {
			continue
		}

		matchedTables = append(matchedTables, tables...)
		tableNames := strings.Join(funk.Map(tables, func(table *model.Table) string {
			return fmt.Sprintf("'%s'", table.TABLE_NAME)
		}).([]string), ",")
		rows, err = c.odb.Query(fmt.Sprintf("select a.owner as table_schema, a.table_name, c.created as create_time from all_tables a left join all_objects c on a.table_name = c.object_name where a.table_name in (%s)", tableNames))
		if err != nil {
			return c, err
		}
		defer rows.Close()
		for rows.Next() {
			object := &model.Table{}
			rows.Scan(&object.TABLE_SCHEMA, &object.TABLE_NAME, &object.CREATE_TIME)
			for _, table := range matchedTables {
				if table.TABLE_NAME == object.TABLE_NAME && table.TABLE_SCHEMA == object.TABLE_SCHEMA {
					table.CREATE_TIME = object.CREATE_TIME
					break
				}
			}
		}
		rows, err = c.odb.Query(fmt.Sprintf("select owner as table_schema, column_name, table_name, nullable, data_type, data_length, data_precision, data_scale from all_tab_columns where table_name in (%s) order by column_id asc", tableNames))
		if err != nil {
			return c, err
		}
		for rows.Next() {
			rows.Columns()
			column := &model.Column{}
			var dataLength uint64
			rows.Scan(&column.TABLE_SCHEMA, &column.COLUMN_NAME, &column.TABLE_NAME, &column.IS_NULLABLE, &column.DATA_TYPE, &dataLength, &column.NUMERIC_PRECISION, &column.NUMERIC_SCALE)
			if column.NUMERIC_PRECISION == 0 {
				column.NUMERIC_PRECISION = dataLength
			}
			column.TABLE_CATALOG = tableRule.DatabasePattern
			if column.IS_NULLABLE == "Y" {
				column.IS_NULLABLE = "YES"
			}
			allColumns = append(allColumns, column)
		}
		rows, err = c.odb.Query(fmt.Sprintf("select a.owner as table_schema, b.constraint_type, a.table_name, a.column_name, a.position from all_cons_columns a left join all_constraints b on a.table_name = b.table_name and a.constraint_name = b.constraint_name and a.owner = b.owner where b.constraint_type = 'P' and a.table_name in (%s)", strings.Join(funk.Map(tables, func(table *model.Table) string {
			return fmt.Sprintf("'%s'", table.TABLE_NAME)
		}).([]string), ",")))
		if err != nil {
			return c, err
		}
		for rows.Next() {
			columnKey := &model.KeyColumnUsage{}
			rows.Scan(&columnKey.TABLE_SCHEMA, &columnKey.CONSTRAINT_NAME, &columnKey.TABLE_NAME, &columnKey.COLUMN_NAME, &columnKey.ORDINAL_POSITION)
			columnKey.TABLE_CATALOG = tableRule.DatabasePattern
			keyColumnUsageRows = append(keyColumnUsageRows, columnKey)
		}
	}
	if len(matchedTables) == 0 {
		return c, errors.New("No matching table columns found.")
	}
	if len(allColumns) == 0 {
		return c, errors.New("Failed to get rows from information_schema.columns.")
	}
	for _, keyColumn := range keyColumnUsageRows {
		if strings.ToLower(keyColumn.CONSTRAINT_NAME) == "p" {
			keyColumn.CONSTRAINT_NAME = "PRIMARY"
		}
	}
	c.calculateRuledTablesMap(matchedTables, allColumns, keyColumnUsageRows)
	if len(c.ruledTablesMap) == 0 {
		return c, errors.New("No matching table columns found.")
	}
	return c, nil
}

func (c *OracleSource) FormatFlinkColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := c.convertType(column.DATA_TYPE, column.NUMERIC_PRECISION, column.NUMERIC_SCALE, true)

	nullableStr := "NULL"
	if column.IS_NULLABLE != "YES" {
		nullableStr = "NOT NULL"
	}
	columnStr := fmt.Sprintf("  `%s` %s %s", column.COLUMN_NAME, colDataType, nullableStr)
	return columnStr, nil
}

func (c *OracleSource) FormatStarRocksColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := c.convertType(column.DATA_TYPE, column.NUMERIC_PRECISION, column.NUMERIC_SCALE, false)
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

func (c *OracleSource) convertType(dt string, NUMERIC_PRECISION, NUMERIC_SCALE uint64, flink bool) string {
	switch strings.TrimSpace(strings.Split(strings.ToLower(dt), "(")[0]) {
	case "char", "nchar", "nvarchar2", "varchar", "varchar2", "clob", "nclob", "xmltype":
		return "STRING"
	case "number":
		if NUMERIC_PRECISION == 1 && NUMERIC_SCALE == 0 {
			return "BOOLEAN"
		}
		if !c.config.UseDecimalV3 && NUMERIC_PRECISION > 27 {
			return "STRING"
		}
		if NUMERIC_SCALE <= 0 {
			if NUMERIC_PRECISION-NUMERIC_SCALE < 3 {
				return "TINYINT"
			}
			if NUMERIC_PRECISION-NUMERIC_SCALE < 5 {
				return "SMALLINT"
			}
			if NUMERIC_PRECISION-NUMERIC_SCALE < 10 {
				return "INT"
			}
			if NUMERIC_PRECISION-NUMERIC_SCALE < 19 {
				return "BIGINT"
			}
		}
		if NUMERIC_PRECISION <= 38 {
			return fmt.Sprintf("DECIMAL(%d, %d)", NUMERIC_PRECISION, NUMERIC_SCALE)
		}
		return "STRING"
	case "float", "binary_float":
		if NUMERIC_PRECISION < 24 {
			return "FLOAT"
		}
		return "DOUBLE"
	case "date":
		if flink {
			return "TIMESTAMP"
		}
		return "DATETIME"
	case "timestamp":
		if flink {
			return dt
		}
		return "DATETIME"
	case "interval":
		return "BIGINT"
	}
	return "STRING"
}

func (c *OracleSource) GetRuledTablesMap() map[*conf.TableRule][]*common.TableColumns {
	return c.ruledTablesMap
}

func (c *OracleSource) GetFlinkConnectorName() string {
	return "oracle-cdc"
}

func (c *OracleSource) GetFlinkSpecialProps(matchedTableRule *conf.TableRule) map[string]string {
	props := map[string]string{
		"debezium.database.tablename.case.insensitive": "false",
		"debezium.log.mining.strategy":                 "online_catalog",
	}
	if strings.Index(c.dbVersion, " 11g ") > 0 || strings.Index(c.dbVersion, " 10g ") > 0 || strings.Index(c.dbVersion, " 9i ") > 0 {
		props["debezium.decimal.handling.mode"] = "STRING"
	}
	if c.cdbMode {
		props["debezium.database.pdb.name"] = matchedTableRule.DatabasePattern
		props["database-name"] = c.cdbName
	}
	return props
}

func (c *OracleSource) CombineSchemaName() bool {
	return true
}

// alter system set db_recovery_file_dest = '/home/oracle/data' scope=spfile;
// create table ortest( k1 varchar2(50) primary key, k2 char(20) default 'aaaa' not null, k3 number(11,0), k6 DATE, k7 timestamp(3) );
// insert into oracle.ortest values('aa','bb',13,to_date('2021-01-01', 'yyyy-mm-dd'),to_timestamp('2022-01-01 00:00:01', 'yyyy-mm-dd hh24:mi:ss'));
// insert into FLINKUSER.TEST20 values('999', null, null,'fdaasdfasf','fdsasdf','sxx   ','111','222','333','444',null,1238712931,12,1,1,0,0,0);

// alter system set db_recovery_file_dest_size = 10G;
// alter system set db_recovery_file_dest = '/xxxxx/xxxx/xxx' scope=spfile;
// shutdown immediate;
// startup mount;
// alter database archivelog;
// alter database open;

// ALTER TABLE oracle.ortest ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
// ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

// GRANT CREATE SESSION TO oracle;
// GRANT SET CONTAINER TO oracle;
// GRANT SELECT ON V_$DATABASE to oracle;
// GRANT FLASHBACK ANY TABLE TO oracle;
// GRANT SELECT ANY TABLE TO oracle;
// GRANT SELECT_CATALOG_ROLE TO oracle;
// GRANT EXECUTE_CATALOG_ROLE TO oracle;
// GRANT SELECT ANY TRANSACTION TO oracle;
// GRANT LOGMINING TO oracle;

// GRANT CREATE TABLE TO oracle;
// GRANT LOCK ANY TABLE TO oracle;
// GRANT ALTER ANY TABLE TO oracle;
// GRANT CREATE SEQUENCE TO oracle;

// GRANT EXECUTE ON DBMS_LOGMNR TO oracle;
// GRANT EXECUTE ON DBMS_LOGMNR_D TO oracle;

// GRANT SELECT ON V_$LOG TO oracle;
// GRANT SELECT ON V_$LOG_HISTORY TO oracle;
// GRANT SELECT ON V_$LOGMNR_LOGS TO oracle;
// GRANT SELECT ON V_$LOGMNR_CONTENTS TO oracle;
// GRANT SELECT ON V_$LOGMNR_PARAMETERS TO oracle;
// GRANT SELECT ON V_$LOGFILE TO oracle;
// GRANT SELECT ON V_$ARCHIVED_LOG TO oracle;
// GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO oracle;
