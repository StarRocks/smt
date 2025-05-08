// +build amd64

package source

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/model"
	"strconv"
	"strings"
	"time"

	"github.com/beltran/gohive"
	"github.com/thoas/go-funk"
)

type HiveSource struct {
	DBSource
	conn *gohive.Connection
}

func (c *HiveSource) Construct(config *conf.Config) IDBSource {
	c.config = config
	for _, tableRule := range c.config.TableRules {
		tableRule.SchemaPattern = ".*"
	}
	return c
}

func (c *HiveSource) Databases() ([]string, error) {
	return c.getDatabases()
}

func (c *HiveSource) Schemas(db string) ([]string, error) {
	return []string{}, nil
}

func (c *HiveSource) Tables(db, _ string) ([]string, error) {
	tables, err := c.getTables([]string{db})
	return funk.Map(tables, func(table *model.Table) string {
		return table.TABLE_NAME
	}).([]string), err
}

func (c *HiveSource) Destroy() {
	if c.conn == nil {
		return
	}
	c.conn.Close()
}

func (c *HiveSource) ResultConventers() int {
	return common.ConvertToStarRocks | common.ConvertToStarRocksExternal | common.ConvertToFlink
}

func (c *HiveSource) Sample(db, _, table string, limit int) ([]map[string]interface{}, error) {
	cursor := c.conn.Cursor()
	ctx := context.Background()
	cursor.Exec(ctx, fmt.Sprintf("SELECT * FROM `%s`.`%s` limit %d", db, table, limit))
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
		return nil, cursor.Err
	}
	defer cursor.Close()
	rows := []map[string]interface{}{}
	for cursor.HasMore(ctx) {
		rows = append(rows, cursor.RowMap(ctx))
	}
	return rows, nil
}

func (c *HiveSource) InitDB() error {
	configuration := gohive.NewConnectConfiguration()
	var connection *gohive.Connection
	var errConn error
	if c.config.DBAuthType == common.DBSourceAuthNone || c.config.DBAuthType == common.DBSourceAuthNoneHTTP {
		configuration.Username = c.config.DBUser
		configuration.Password = c.config.DBPassword
		if c.config.DBAuthType == common.DBSourceAuthNoneHTTP {
			configuration.HTTPPath = "cliservice"
			configuration.TransportMode = "http"
		}
		connection, errConn = gohive.Connect(c.config.DBHost, int(c.config.DBPort), "NONE", configuration)
	} else if c.config.DBAuthType == common.DBSourceAuthKerberos || c.config.DBAuthType == common.DBSourceAuthKerberosHTTP {
		tmp := strings.Split(c.config.DBUser, "/")
		service, host := tmp[0], tmp[1]
		os.Setenv("SERVICE_HOST_QUALIFIED", host)
		configuration.Service = service

		if c.config.DBAuthType == common.DBSourceAuthKerberosHTTP {
			configuration.HTTPPath = "cliservice"
			configuration.TransportMode = "http"
		}
		connection, errConn = gohive.Connect(c.config.DBHost, int(c.config.DBPort), "KERBEROS", configuration)
	} else if c.config.DBAuthType == common.DBSourceAuthNoSasl {
		connection, errConn = gohive.Connect(c.config.DBHost, int(c.config.DBPort), "NOSASL", configuration)
	} else if c.config.DBAuthType == common.DBSourceAuthZK {
		connection, errConn = gohive.ConnectZookeeper(c.config.DBHost, "NONE", configuration)
	} else if c.config.DBAuthType == common.DBSourceAuthLDAP {
		configuration.Username = c.config.DBUser
		configuration.Password = c.config.DBPassword
		connection, errConn = gohive.Connect(c.config.DBHost, int(c.config.DBPort), "LDAP", configuration)
	}
	if errConn != nil {
		log.Fatal(errConn)
	}
	c.conn = connection
	return nil
}

func (c *HiveSource) Build() (IDBSourceProvider, error) {
	dbList, err := c.getDatabases()
	if err != nil {
		return c, errors.New(err.Error())
	}
	tables, err := c.getTables(dbList)
	if err != nil {
		return c, errors.New(err.Error())
	}
	matchedTables := []*model.Table{}
	allColumns := []*model.Column{}
	keyColumnUsageRows := []*model.KeyColumnUsage{}
	for _, tableRule := range c.config.TableRules {
		tableMap := map[string]bool{}
		for _, table := range tables {
			key := fmt.Sprintf("`%s`.`%s`", table.TABLE_SCHEMA, table.TABLE_NAME)
			if _, ok := tableMap[key]; ok {
				continue
			}
			if !common.RegMatchString(tableRule.DatabasePattern, table.TABLE_SCHEMA) || !common.RegMatchString(tableRule.TablePattern, table.TABLE_NAME) {
				continue
			}
			columns, kcuList, err := c.describeTable(table)
			if err != nil {
				return c, errors.New(err.Error())
			}
			matchedTables = append(matchedTables, table)
			allColumns = append(allColumns, columns...)
			keyColumnUsageRows = append(keyColumnUsageRows, kcuList...)
			tableMap[key] = true
		}
	}
	c.calculateRuledTablesMap(matchedTables, allColumns, keyColumnUsageRows)
	if len(c.ruledTablesMap) == 0 {
		return c, errors.New("No matching table columns found.")
	}
	return c, nil
}

func (c *HiveSource) GetRuledTablesMap() map[*conf.TableRule][]*common.TableColumns {
	return c.ruledTablesMap
}

func (c *HiveSource) FormatFlinkColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := "STRING"
	switch column.DATA_TYPE {
	case "char", "varchar", "string":
		colDataType = "STRING"
		break
	case "boolean", "tinyint", "smallint", "int", "bigint", "float", "double", "date":
		colDataType = strings.ToUpper(column.DATA_TYPE)
		break
	case "integer":
		colDataType = "INT"
		break
	case "decimal":
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
	case "time", "datetime", "timestamp":
		colDataType = "TIMESTAMP"
		break
	}
	nullableStr := "NULL"
	columnStr := fmt.Sprintf("  `%s` %s %s", column.COLUMN_NAME, colDataType, nullableStr)
	return columnStr, nil
}

func (c *HiveSource) FormatStarRocksColumnDef(table *model.Table, column *model.Column) (string, error) {
	colDataType := "STRING"
	switch column.DATA_TYPE {
	case "char", "varchar", "string":
		colDataType = "STRING"
		break
	case "boolean", "tinyint", "smallint", "int", "bigint", "float", "double", "date":
		colDataType = strings.ToUpper(column.DATA_TYPE)
		break
	case "integer":
		colDataType = "INT"
		break
	case "decimal":
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
	case "time", "datetime", "timestamp":
		colDataType = "DATETIME"
		break
	}

	nullableStr := "NULL"
	defaultStr := ""
	columnStr := fmt.Sprintf("  `%s` %s %s %s COMMENT \"%s\"", column.COLUMN_NAME, colDataType, nullableStr, defaultStr, c.encodeComment(column.COLUMN_COMMENT))
	return columnStr, nil
}

func (c *HiveSource) GetFlinkConnectorName() string {
	return ""
}

func (c *HiveSource) GetFlinkSpecialProps(matchedTableRule *conf.TableRule) map[string]string {
	return nil
}

func (c *HiveSource) CombineSchemaName() bool {
	return false
}

func (c *HiveSource) GetMetaStoreURI() (string, error) {
	cursor := c.conn.Cursor()
	ctx := context.Background()
	cursor.Exec(ctx, fmt.Sprintf("set hive.metastore.uris"))
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
		return "", cursor.Err
	}
	defer cursor.Close()
	var uri string
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &uri)
	}
	return uri, nil
}

func (c *HiveSource) getDatabases() ([]string, error) {
	cursor := c.conn.Cursor()
	ctx := context.Background()
	cursor.Exec(ctx, "show databases")
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
		return nil, cursor.Err
	}
	defer cursor.Close()
	dbList := []string{}
	var db string
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &db)
		if cursor.Err != nil {
			return nil, cursor.Err
		}
		dbList = append(dbList, db)
	}
	return dbList, nil
}

func (c *HiveSource) getTables(dbList []string) ([]*model.Table, error) {
	tables := []*model.Table{}
	for _, db := range dbList {
		cursor := c.conn.Cursor()
		ctx := context.Background()
		cursor.Exec(ctx, fmt.Sprintf("show tables from %s", db))
		if cursor.Err != nil {
			log.Fatal(cursor.Err)
			return nil, cursor.Err
		}
		defer cursor.Close()
		var table string
		for cursor.HasMore(ctx) {
			cursor.FetchOne(ctx, &table)
			if cursor.Err != nil {
				return nil, cursor.Err
			}
			tables = append(tables, &model.Table{
				ModelBase: model.ModelBase{
					TABLE_CATALOG: db,
					TABLE_SCHEMA:  db,
					TABLE_NAME:    table,
				},
			})
		}
	}
	return tables, nil
}

func (c *HiveSource) describeTable(table *model.Table) ([]*model.Column, []*model.KeyColumnUsage, error) {
	cursor := c.conn.Cursor()
	ctx := context.Background()
	cursor.Exec(ctx, fmt.Sprintf("describe formatted `%s`.`%s`", table.TABLE_SCHEMA, table.TABLE_NAME))
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
		return nil, nil, cursor.Err
	}
	defer cursor.Close()
	var col1 string
	var col2 string
	var col3 string
	var colAsPartition bool
	var parentType string
	columns := []*model.Column{}
	kcuList := []*model.KeyColumnUsage{}
	precisionReg, _ := regexp.Compile("[0-9]+")
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &col1, &col2, &col3)
		if len(col1) > 0 && strings.Index(parentType, "# col_name") == 0 && !strings.HasPrefix(col1, "# ") {
			column := &model.Column{
				ModelBase: model.ModelBase{
					TABLE_CATALOG: table.TABLE_CATALOG,
					TABLE_SCHEMA:  table.TABLE_SCHEMA,
					TABLE_NAME:    table.TABLE_NAME,
				},
				COLUMN_NAME:    col1,
				DATA_TYPE:      col2,
				COLUMN_COMMENT: col3,
			}
			if strings.HasPrefix(column.DATA_TYPE, "char(") {
				column.NUMERIC_PRECISION, _ = strconv.ParseUint(string(precisionReg.Find([]byte(column.COLUMN_NAME))), 10, 64)
				column.DATA_TYPE = "char"
			}
			if strings.HasPrefix(column.DATA_TYPE, "varchar(") {
				column.NUMERIC_PRECISION, _ = strconv.ParseUint(string(precisionReg.Find([]byte(column.COLUMN_NAME))), 10, 64)
				column.DATA_TYPE = "varchar"
			}
			if strings.HasPrefix(column.DATA_TYPE, "decimal(") {
				matches := precisionReg.FindAll([]byte(column.DATA_TYPE), -1)
				column.NUMERIC_PRECISION, _ = strconv.ParseUint(string(matches[0]), 10, 64)
				column.NUMERIC_SCALE, _ = strconv.ParseUint(string(matches[1]), 10, 64)
				column.DATA_TYPE = "decimal"
			}
			if colAsPartition {
				column.IsInPartitionKey = true
			}
			columns = append(columns, column)
		}
		if len(col1) == 0 && strings.Contains(parentType, "Table Parameters") {
			if strings.Contains(col2, "numRows") {
				table.TABLE_ROWS, _ = strconv.ParseUint(strings.Trim(col3, " "), 10, 64)
			} else if strings.Contains(col2, "rawDataSize") {
				table.DATA_LENGTH, _ = strconv.ParseUint(strings.Trim(col3, " "), 10, 64)
			} else if strings.Contains(col2, "totalSize") {
				totalSize, _ := strconv.ParseUint(strings.Trim(col3, " "), 10, 64)
				table.INDEX_LENGTH = totalSize - table.DATA_LENGTH
			} else if strings.Contains(col2, "comment") {
				table.TABLE_COMMENT = strings.Trim(col3, " ")
			}
		}
		if strings.Contains(col1, "CreateTime:") {
			table.CREATE_TIME, _ = time.Parse("Mon Jan 2 15:04:05 MST 2006", strings.Trim(col2, " "))
		}
		if strings.Contains(col1, "Bucket Columns:") {
			col2 = strings.Replace(col2, "[", "", -1)
			col2 = strings.Replace(col2, "]", "", -1)
			for idx, k := range strings.Split(col2, ",") {
				if len(strings.Trim(k, " ")) == 0 {
					continue
				}
				kcuList = append(kcuList, &model.KeyColumnUsage{
					ModelBase: model.ModelBase{
						TABLE_CATALOG: table.TABLE_CATALOG,
						TABLE_SCHEMA:  table.TABLE_SCHEMA,
						TABLE_NAME:    table.TABLE_NAME,
					},
					COLUMN_NAME:      strings.Trim(k, " "),
					CONSTRAINT_NAME:  "bucket",
					ORDINAL_POSITION: uint64(idx),
				})
			}
		}
		if strings.Contains(col1, "# Partition Information") {
			colAsPartition = true
		}
		if len(col1) > 0 && strings.Index(col1, "#") == 0 || strings.Contains(col1, "Table Parameters") {
			parentType = col1
		}
	}
	return columns, kcuList, nil
}

// Create table dz_test(
//     id  bigint  comment'1',
//     v2 char(10) comment'2',
//     v3 varchar(11) comment'3',
//     v4 string comment'4',
//     v5 BOOLEAN comment'5',
//     v6 TINYINT comment'6',
//     v7 SMALLINT comment'7',
//     v8 INT comment'8',
//     v9 bigint comment'9',
//     v10 FLOAT comment'10',
//     v11 DOUBLE comment'11',
//     v12 DECIMAL(20, 1) comment'12',
//     v13 DATE comment'13',
//     v14 TIMESTAMP	comment'14',
//     v15 BINARY,
//     v16 ARRAY<INT> comment'16',
//     v17 MAP<string, string> comment'17',
//     v18 STRUCT<arg1 : int> comment'18'
// ) comment 'test'
// Partitioned by(dt date)
// Clustered by(id)
// Sorted by(id)
// Into 4 buckets
// stored as PARQUET;

// insert into table dz_test partition(dt='2021-01-01') select 1, 'a', '22', '333', true, 1,2,3,4,5.1, 6.1, 777.2, '2020-01-01', '2021-01-01 00:00:01', 'dev', array(22,33), map('a','a','b','b'), named_struct('arg1', 1);

// Create table dz_test1(
//     id  bigint  comment'1',
//     v2 char(10) comment'2',
//     v3 varchar(11) comment'3',
//     v4 string comment'4',
//     v5 BOOLEAN comment'5',
//     v6 TINYINT comment'6',
//     v7 SMALLINT comment'7',
//     v8 INT comment'8',
//     v9 bigint comment'9',
//     v10 FLOAT comment'10',
//     v11 DOUBLE comment'11',
//     v12 DECIMAL(20, 1) comment'12',
//     v13 DATE comment'13',
//     v14 TIMESTAMP	comment'14',
//     v15 BINARY
// ) comment 'test'
// Partitioned by(dt date)
// Clustered by(id)
// Sorted by(id)
// Into 4 buckets
// stored as PARQUET;

// insert into table dz_test1 partition(dt='2021-01-01') select 1, 'a', '22', '333', true, 1,2,3,4,5.1, 6.1, 777.2, '2020-01-01', '2021-01-01 00:00:01', 'dev';
