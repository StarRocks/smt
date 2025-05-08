package model

import "time"

// Table information_schema.tables
type Table struct {
	ModelBase
	TABLE_TYPE      string    `gorm:"type:varchar(64);column:table_type" json:"tableType"`
	ENGINE          string    `gorm:"type:varchar(64);column:engine" json:"engine"`
	ROW_FORMAT      string    `gorm:"type:varchar(10);column:row_format" json:"rowFormat"`
	TABLE_ROWS      uint64    `gorm:"type:bigint(21);column:table_rows" json:"tableRows"`
	AVG_ROW_LENGTH  uint64    `gorm:"type:bigint(21);column:avg_row_length" json:"avgRowLength"`
	INDEX_LENGTH    uint64    `gorm:"type:bigint(21);column:index_length" json:"indexLength"`
	DATA_LENGTH     uint64    `gorm:"type:bigint(21);column:data_length" json:"dataLength"`
	MAX_DATA_LENGTH uint64    `gorm:"type:bigint(21);column:max_data_length" json:"maxDataLength"`
	AUTO_INCREMENT  uint64    `gorm:"type:bigint(21);column:auto_increment" json:"autoIncrement"`
	CREATE_TIME     time.Time `gorm:"type:datetime;column:create_time" json:"createTime"`
	TABLE_COMMENT   string    `gorm:"type:varchar(2048);column:table_comment" json:"tableComment"`
	// clickhouse
	UUID string `gorm:"type:varchar(2048);column:uuid" json:"uuid"`
}

func (Table) TableName() string {
	return "information_schema.tables"
}
