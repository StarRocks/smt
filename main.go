package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"starrocks-migrate-tool/common"
	"starrocks-migrate-tool/conf"
	"starrocks-migrate-tool/convert"
	"starrocks-migrate-tool/source"
	"strings"

	"gorm.io/gorm"
)

var db *gorm.DB
var config *conf.Config

func main() {
	config = &conf.Config{}
	_, err := config.Load()
	if err != nil {
		panic(err)
	}

	source := source.Create(config)
	if source == nil {
		panic("Failed to create db source.")
	}

	err = source.InitDB()
	if err != nil {
		panic(err)
	}

	dbProvider, err := source.Build()
	if err != nil {
		panic(err)
	}

	fmt.Println(fmt.Sprintf("Successfully got tables from the source database. Converting them to StarRocks DDL..."))
	converters := []convert.IConverter{}
	if dbProvider.ResultConventers()&common.ConvertToStarRocks == common.ConvertToStarRocks {
		// convert to starrocks ddl
		converters = append(converters, new(convert.StarRocks).Construct(config, dbProvider))
	}
	if dbProvider.ResultConventers()&common.ConvertToStarRocksExternal == common.ConvertToStarRocksExternal {
		// convert to starrocks external ddl
		converters = append(converters, new(convert.StarRocksExternal).Construct(config, dbProvider))
	}
	if dbProvider.ResultConventers()&common.ConvertToFlink == common.ConvertToFlink {
		// convert to flink ddl
		converters = append(converters, new(convert.Flink).Construct(config, dbProvider))
	}
	if len(config.OutputDir) == 0 {
		config.OutputDir = "./result"
	}
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	writeDir := filepath.Join(dir, config.OutputDir)
	os.RemoveAll(writeDir)
	os.MkdirAll(writeDir, 0766)
	for _, cvter := range converters {
		ddlList, ruledDDLMap, err := cvter.ToCreateDDL()
		filePrefix := cvter.ResultFilePrefix()
		fmt.Println(fmt.Sprintf("Writing starrocks ddl reults..."))
		if err != nil {
			panic(err)
		}

		err = writeFile(ddlList, writeDir, filePrefix+".all.sql")
		if err != nil {
			panic(err)
		}

		for seq, ddls := range ruledDDLMap {
			err = writeFile(ddls, writeDir, fmt.Sprintf("%s.%s.sql", filePrefix, seq))
			if err != nil {
				panic(err)
			}
		}
		fmt.Println(fmt.Sprintf("Done writing to: %s", writeDir))
	}
}

func writeFile(ddlList []string, writeDir, fileName string) error {
	return ioutil.WriteFile(filepath.Join(writeDir, fileName), []byte(strings.Join(ddlList, ";\n\n")+";\n"), 0644)
}
