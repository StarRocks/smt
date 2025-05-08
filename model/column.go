package model

// Column information_schema.columns
type Column struct {
	ModelBase
	COLUMN_NAME        string  `gorm:"type:varchar(64);column:column_name" json:"columnName"`
	ORDINAL_POSITION   uint64  `gorm:"type:bigint(21);column:ordinal_position" json:"ordinalPosition"`
	COLUMN_DEFAULT     *string `gorm:"type:longtext;column:column_default" json:"columnDefault"`
	IS_NULLABLE        string  `gorm:"type:varchar(3);column:is_nullable" json:"isNullable"`
	DATA_TYPE          string  `gorm:"type:varchar(64);column:data_type" json:"dataType"`
	NUMERIC_PRECISION  uint64  `gorm:"type:bigint(21);column:numeric_precision" json:"numericPrecision"`
	NUMERIC_SCALE      uint64  `gorm:"type:bigint(21);column:numeric_scale" json:"numericScale"`
	DATETIME_PRECISION uint64  `gorm:"type:bigint(21);column:datetime_precision" json:"datatimePrecision"`
	COLUMN_TYPE        string  `gorm:"type:longtext;column:column_type" json:"columnType"`
	COLUMN_KEY         string  `gorm:"type:varchar(3);column:column_key" json:"columnKey"`
	COLUMN_COMMENT     string  `gorm:"type:varchar(1024);column:column_comment" json:"columnComment"`
	UDT_NAME           string  `gorm:"type:varchar(64);column:udt_name" json:"udtName"`
	// clickhouse, hive
	IsInPartitionKey bool `gorm:"type:int;column:is_in_partition_key" json:"isPartitionKey"`
	// clickhouse
	IsInSortingKey bool `gorm:"type:int;column:is_in_sorting_key" json:"isSortingKey"`
	// // clickhouse subset of the sorting keys
	// IsInPrimaryKey bool `gorm:"type:int;column:is_in_primary_key" json:"isPrimaryKey"`
	// clickhouse
	IsInSamplingKey bool `gorm:"type:int;column:is_in_sampling_key" json:"isInSamplingKey"`
}

func (Column) TableName() string {
	return "information_schema.columns"
}
