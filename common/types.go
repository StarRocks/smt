package common

import (
	"errors"
	"starrocks-migrate-tool/model"
)

const (
	GIGA_BYTES              = 1024 * 1024 * 1024
	DAY_SECONDS             = 24 * 3600
	MONTH_SECONDS           = 30 * 24 * 3600
	DATE_TEMPLATE           = "2006-01-02"
	DATETIME_TEMPLATE       = "2006-01-02 15:04:05"
	SMALL_DATETIME_TEMPLATE = "2006-01-02 15:04"
	DATETIME_ISO_TEMPLATE   = "2006-01-02T15:04:05Z"
	TIME_TEMPLATE           = "15:04:05"
	SHARD_SUFFIX            = "_auto_shard"
)

type TableColumns struct {
	Table      *model.Table
	Columns    []*model.Column
	PrimaryKCU []*model.KeyColumnUsage
	UniqueKCU  []*model.KeyColumnUsage
}

type DBSourceType int

const (
	DBSourceUnknow DBSourceType = iota
	DBSourceMySQL
	DBSourcePostgreSQL
	DBSourceOracle
	DBSourceSQLServer
	DBSourceClickHouse
	DBSourceHive
	DBSourceTiDB
)

var dbSourceTypeMap = map[string]DBSourceType{
	"mysql":      DBSourceMySQL,
	"pgsql":      DBSourcePostgreSQL,
	"oracle":     DBSourceOracle,
	"sqlserver":  DBSourceSQLServer,
	"clickhouse": DBSourceClickHouse,
	"hive":       DBSourceHive,
	"tidb":       DBSourceTiDB,
}

func ParseDBSourceType(name string) (DBSourceType, error) {
	if sourceType, ok := dbSourceTypeMap[name]; ok {
		return sourceType, nil
	}

	return DBSourceUnknow, errors.New("Unsupported db source.")
}

type DBSourceAuthType int

const (
	DBSourceAuthUnknow DBSourceAuthType = iota
	DBSourceAuthKerberos
	DBSourceAuthNoSasl
	DBSourceAuthNone
	DBSourceAuthNoneHTTP
	DBSourceAuthKerberosHTTP
	DBSourceAuthZK
	DBSourceAuthLDAP
)

var dbSourceAuthTypeMap = map[string]DBSourceAuthType{
	"kerberos":      DBSourceAuthKerberos,
	"nosasl":        DBSourceAuthNoSasl,
	"none":          DBSourceAuthNone,
	"none_http":     DBSourceAuthNoneHTTP,
	"kerberos_http": DBSourceAuthKerberosHTTP,
	"zk":            DBSourceAuthZK,
	"ldap":          DBSourceAuthLDAP,
}

func ParseDBSourceAuthType(name string) (DBSourceAuthType, error) {
	if sourceType, ok := dbSourceAuthTypeMap[name]; ok {
		return sourceType, nil
	}

	return DBSourceAuthUnknow, errors.New("Unsupported db authentication type.")
}

const (
	ConvertToFlink = 1 << iota
	ConvertToStarRocks
	ConvertToStarRocksExternal
)
