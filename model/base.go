package model

import "fmt"

type ModelBase struct {
	TABLE_CATALOG string `gorm:"type:varchar(64);column:table_catalog" json:"tableCatalog"`
	TABLE_SCHEMA  string `gorm:"type:varchar(64);column:table_schema" json:"tableSchema"`
	TABLE_NAME    string `gorm:"type:varchar(64);column:table_name" json:"tableName"`
}

func (c *ModelBase) GetSchemaPrefixedTableName() string {
	return fmt.Sprintf("%s__%s", c.TABLE_SCHEMA, c.TABLE_NAME)
}
