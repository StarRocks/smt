package conf

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"starrocks-migrate-tool/common"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/unknwon/goconfig"
)

// Config of the service
type Config struct {

	// database
	DBHost         string
	DBPort         int64
	DBUser         string
	DBPassword     string
	DBType         common.DBSourceType
	DBAuthType     common.DBSourceAuthType
	UseDecimalV3   bool
	BENum          int64
	ReplicationNum int64
	TableRules     []*TableRule

	// output
	OutputDir string

	// config file
	ConfigPath string
}

type TableRule struct {
	Seq                string
	DatabasePattern    string
	SchemaPattern      string
	TablePattern       string
	PartitionKey       string
	Partitions         string
	DuplicateKeys      string
	DistributedBy      string
	Buckets            int64
	FromShardingSrc    bool
	Properties         map[string]string
	ExternalProperties map[string]string
	FlinkSinkProps     map[string]string
	FlinkSourceProps   map[string]string
}

// Load configurations
func (config *Config) Load() (*Config, error) {
	// set default config path
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	defaultConfigPath := dir + ConfigFilePath
	// parse config path from command line
	flag.StringVar(&config.ConfigPath, "c", defaultConfigPath, "Set config path: [/path/to/xxx.conf]")
	flag.Parse()
	c, e := config.readProps()
	return c, e
}

func (config *Config) readProps() (*Config, error) {
	file, err := goconfig.LoadConfigFile(strings.Split(config.ConfigPath, ",")[0])
	if err != nil {
		return nil, err
	}
	if config.DBHost, err = file.GetValue("db", "host"); err != nil {
		return nil, err
	}
	if config.DBPort, err = file.Int64("db", "port"); err != nil {
		return nil, err
	}
	if config.DBUser, err = file.GetValue("db", "user"); err != nil {
		return nil, err
	}
	if config.DBPassword, err = file.GetValue("db", "password"); err != nil {
		return nil, err
	}
	if config.OutputDir, err = file.GetValue("other", "output_dir"); err != nil {
		return nil, err
	}
	if config.BENum, err = file.Int64("other", "be_num"); err != nil {
		return nil, err
	}
	if config.UseDecimalV3, err = file.Bool("other", "use_decimal_v3"); err != nil {
		return nil, err
	}
	config.TableRules = []*TableRule{}
	// parse table rules
	config.ReplicationNum = int64(3)
	if config.BENum < 3 {
		config.ReplicationNum = config.BENum
	}

	config.DBType = common.DBSourceMySQL
	dbType, _ := file.GetValue("db", "type")
	config.DBType, err = common.ParseDBSourceType(dbType)
	if err != nil {
		return nil, err
	}

	for _, sec := range file.GetSectionList() {
		if strings.Index(sec, "table-rule.") == 0 {
			rule := &TableRule{
				Seq:                strings.Replace(sec, "table-rule.", "", -1),
				FlinkSinkProps:     map[string]string{},
				FlinkSourceProps:   map[string]string{},
				Properties:         map[string]string{},
				ExternalProperties: map[string]string{},
			}
			if rule.DatabasePattern, err = file.GetValue(sec, "database"); err != nil {
				return nil, fmt.Errorf("config [%s].database not found", sec)
			}
			if rule.SchemaPattern, err = file.GetValue(sec, "schema"); err != nil {
				if config.DBType == common.DBSourcePostgreSQL || config.DBType == common.DBSourceOracle || config.DBType == common.DBSourceSQLServer {
					return nil, fmt.Errorf("config [%s].schema not found", sec)
				}
			}
			if rule.TablePattern, err = file.GetValue(sec, "table"); err != nil {
				return nil, fmt.Errorf("config [%s].table not found", sec)
			}
			if len(rule.SchemaPattern) == 0 {
				rule.SchemaPattern = ".*"
			}
			rule.PartitionKey, _ = file.GetValue(sec, "partition_key")
			rule.Partitions, _ = file.GetValue(sec, "partitions")
			rule.DuplicateKeys, _ = file.GetValue(sec, "duplicate_keys")
			rule.DistributedBy, _ = file.GetValue(sec, "distributed_by")
			rule.Buckets, _ = file.Int64(sec, "bucket_num")
			secKeyVals, err := file.GetSection(sec)
			if err != nil {
				return nil, err
			}
			for key, val := range secKeyVals {
				if strings.Index(key, "properties.") == 0 {
					rule.Properties[strings.Replace(key, "properties.", "", -1)] = val
					continue
				}
				if strings.Index(key, "external.properties.") == 0 {
					rule.ExternalProperties[strings.Replace(key, "external.properties.", "", -1)] = val
					continue
				}
				if strings.Index(key, "flink.starrocks.") == 0 {
					rule.FlinkSinkProps[strings.Replace(key, "flink.starrocks.", "", -1)] = val
					continue
				}
				if strings.Index(key, "flink.cdc.") == 0 {
					rule.FlinkSourceProps[strings.Replace(key, "flink.cdc.", "", -1)] = val
					continue
				}
			}
			rule.Properties["replication_num"] = strconv.FormatInt(config.ReplicationNum, 10)
			rule.FromShardingSrc = true
			config.TableRules = append(config.TableRules, rule)
		}
	}

	if config.DBType == common.DBSourceHive {
		config.DBAuthType = common.DBSourceAuthNone
		dbAuthType, _ := file.GetValue("db", "authentication")
		config.DBAuthType, err = common.ParseDBSourceAuthType(dbAuthType)
		if err != nil {
			return nil, err
		}
	}

	glog.Infof("loading configurations: [%v]", config)

	return config, nil
}
