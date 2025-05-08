package convert

import (
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/model"
	"starrocks-migrate-tool/source"

	funk "github.com/thoas/go-funk"
)

// Converter Converter interface
type IConverter interface {
	Construct(config *conf.Config, dbProvider source.IDBSourceProvider) IConverter
	ToCreateDDL() ([]string, map[string][]string, error)
	ResultFilePrefix() string
}

// Converter service struct
type Converter struct {
	config     *conf.Config
	dbProvider source.IDBSourceProvider
}

func (c *Converter) reorderTableColumns(keys []*model.KeyColumnUsage, columns []*model.Column) (newKeyList []string, reorderedColumns []*model.Column) {
	keyList := []string{}
	uniqueKeyGroups := map[string][]string{}
	constraintNames := []string{}
	for _, key := range keys {
		if _, ok := uniqueKeyGroups[key.CONSTRAINT_NAME]; !ok {
			constraintNames = append(constraintNames, key.CONSTRAINT_NAME)
			uniqueKeyGroups[key.CONSTRAINT_NAME] = []string{}
		}
		uniqueKeyGroups[key.CONSTRAINT_NAME] = append(uniqueKeyGroups[key.CONSTRAINT_NAME], key.COLUMN_NAME)
	}

	for _, name := range constraintNames {
		if len(funk.Filter(columns, func(col *model.Column) bool {
			return col.IS_NULLABLE == "YES" && funk.ContainsString(uniqueKeyGroups[name], col.COLUMN_NAME)
		}).([]*model.Column)) > 0 {
			continue
		}
		keyList = uniqueKeyGroups[name]
		break
	}
	if len(keyList) == 0 {
		return keyList, columns
	}

	// reorder tableColumns.Columns
	uniqCols := funk.Map(keyList, func(key string) *model.Column {
		return funk.Find(columns, func(col *model.Column) bool {
			return col.COLUMN_NAME == key
		}).(*model.Column)
	}).([]*model.Column)
	noneUniqCols := funk.Filter(columns, func(col *model.Column) bool {
		return !funk.Contains(keyList, col.COLUMN_NAME)
	}).([]*model.Column)
	return keyList, append(uniqCols, noneUniqCols...)
}
