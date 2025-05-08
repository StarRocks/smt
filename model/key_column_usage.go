package model

// KeyColumnUsage information_schema.key_column_usage
type KeyColumnUsage struct {
	ModelBase
	COLUMN_NAME      string `gorm:"type:varchar(64);column:column_name" json:"columnName"`
	CONSTRAINT_NAME  string `gorm:"type:varchar(64);column:constraint_name" json:"contraintName"`
	ORDINAL_POSITION uint64 `gorm:"type:bigint(2);column:ordinal_position" json:"ordinalPosition"`
}

func (KeyColumnUsage) TableName() string {
	return "information_schema.key_column_usage"
}
