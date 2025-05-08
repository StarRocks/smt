package model

// Statistics information_schema.statistics
type Statistics struct {
	ModelBase
	NON_UNIQUE   bool   `gorm:"type:bigint(1);column:non_unique" json:"nonUnique"`
	SEQ_IN_INDEX uint64 `gorm:"type:bigint(2);column:seq_in_index" json:"seqInIndex"`
	COLUMN_NAME  string `gorm:"type:varchar(64);column:column_name" json:"columnName"`
}
