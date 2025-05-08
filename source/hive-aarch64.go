// +build !amd64

package source

import (
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/model"
)

type HiveSource struct {
	DBSource
}

func (c *HiveSource) Construct(config *conf.Config) IDBSource {
	c.config = config
	return c
}

func (c *HiveSource) Databases() ([]string, error) {
	return nil, nil
}

func (c *HiveSource) Schemas(db string) ([]string, error) {
	return nil, nil
}

func (c *HiveSource) Tables(db, schema string) ([]string, error) {
	return nil, nil
}

func (c *HiveSource) Sample(_, _, _ string, _ int) ([]map[string]interface{}, error) {
	rows := []map[string]interface{}{}
	return rows, nil
}

func (c *HiveSource) Destroy() {
}

func (c *HiveSource) ResultConventers() int {
	return 0
}

func (c *HiveSource) InitDB() error {
	return nil
}

func (c *HiveSource) Build() (IDBSourceProvider, error) {
	return c, nil
}

func (c *HiveSource) GetRuledTablesMap() map[*conf.TableRule][]*common.TableColumns {
	return c.ruledTablesMap
}

func (c *HiveSource) FormatFlinkColumnDef(table *model.Table, column *model.Column) (string, error) {
	return "", nil
}

func (c *HiveSource) FormatStarRocksColumnDef(table *model.Table, column *model.Column) (string, error) {
	return "", nil
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
	return "", nil
}
