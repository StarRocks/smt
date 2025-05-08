package source

import (
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
)

type TiDBSource struct {
	MySQLSource
}

func (c *TiDBSource) Construct(config *conf.Config) IDBSource {
	c.MySQLSource.Construct(config)
	return c
}

func (c *TiDBSource) Build() (IDBSourceProvider, error) {
	c.MySQLSource.Build()
	return c, nil
}

func (c *TiDBSource) ResultConventers() int {
	return common.ConvertToStarRocks | common.ConvertToStarRocksExternal | common.ConvertToFlink
}

func (c *TiDBSource) GetFlinkConnectorName() string {
	return "tidb-cdc"
}

func (c *TiDBSource) GetFlinkSpecialProps(matchedTableRule *conf.TableRule) map[string]string {
	specialProps := make(map[string]string)
	if _, ok := matchedTableRule.FlinkSourceProps["pd-addresses"]; !ok {
		pdAddresses, _ := c.pdInstance()
		specialProps["pd-addresses"] = pdAddresses
	}

	return specialProps
}

func (c *TiDBSource) pdInstance() (string, error) {
	results := map[string]interface{}{}
	err := c.db.Raw("select INSTANCE as instance from information_schema.cluster_info where TYPE=\"pd\" order by START_TIME DESC LIMIT 1").Find(&results).Error
	if err != nil {
		return "", err
	}

	return results["instance"].(string), nil
}
