package common

import (
	"testing"
)

func TestParseDBSourceAuthType(t *testing.T) {
	tests := []struct {
		name    string
		want    DBSourceAuthType
		wantErr bool
	}{
		{name: "kerberos", want: DBSourceAuthKerberos, wantErr: false},
		{name: "nosasl", want: DBSourceAuthNoSasl, wantErr: false},
		{name: "none", want: DBSourceAuthNone, wantErr: false},
		{name: "none_http", want: DBSourceAuthNoneHTTP, wantErr: false},
		{name: "kerberos_http", want: DBSourceAuthKerberosHTTP, wantErr: false},
		{name: "zk", want: DBSourceAuthZK, wantErr: false},
		{name: "ldap", want: DBSourceAuthLDAP, wantErr: false},
		{name: "know", want: DBSourceAuthUnknow, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDBSourceAuthType(tt.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDBSourceAuthType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseDBSourceAuthType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseDBSourceType(t *testing.T) {
	tests := []struct {
		name    string
		want    DBSourceType
		wantErr bool
	}{
		{name: "mysql", want: DBSourceMySQL, wantErr: false},
		{name: "pgsql", want: DBSourcePostgreSQL, wantErr: false},
		{name: "oracle", want: DBSourceOracle, wantErr: false},
		{name: "sqlserver", want: DBSourceSQLServer, wantErr: false},
		{name: "clickhouse", want: DBSourceClickHouse, wantErr: false},
		{name: "hive", want: DBSourceHive, wantErr: false},
		{name: "tidb", want: DBSourceTiDB, wantErr: false},
		{name: "rocksdb", want: DBSourceUnknow, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDBSourceType(tt.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDBSourceType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseDBSourceType() = %v, want %v", got, tt.want)
			}
		})
	}
}
