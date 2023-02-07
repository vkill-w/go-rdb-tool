package helper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/vkill-w/go-rdb-tool/bytefmt"
	"github.com/vkill-w/go-rdb-tool/core"
	"github.com/vkill-w/go-rdb-tool/model"
	"log"
	"os"
	"time"
)

// ToES read rdb file and convert to elasticsearch
func ToES(rdbFilename, esUrl, indexName, instaceName string, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if esUrl == "" {
		return errors.New("output file path is required")
	}
	// open file
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	// close file
	defer func() {
		_ = rdbFile.Close()
	}()

	//创建es client
	cfg := elasticsearch.Config{
		Addresses: []string{
			esUrl,
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("create  es client  failed, %v", err)
	}
	// create decoder
	var dec decoder = core.NewDecoder(rdbFile)
	var regexOpt RegexOption
	for _, opt := range options {
		switch o := opt.(type) {
		case RegexOption:
			regexOpt = o
		}
	}
	if regexOpt != nil {
		dec, err = regexWrapper(dec, *regexOpt)
		if err != nil {
			return err
		}
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         indexName,        // The default index name
		Client:        es,               // The Elasticsearch client
		NumWorkers:    4,                // The number of worker goroutines
		FlushBytes:    500000,           // The flush threshold in bytes
		FlushInterval: 30 * time.Second, // The periodic flush interval
	})
	err = dec.Parse(func(object model.RedisObject) bool {
		//ES存储对象拼装
		addDataToBulkIndexer(object, bi, instaceName)
		return true
	})
	if err := bi.Close(context.Background()); err != nil {
		log.Printf("Unexpected error: %s", err)
	}
	bi.Stats()
	biStats := bi.Stats()
	biStatsJson, err := json.Marshal(biStats)
	if err != nil {
		return err
	}
	log.Printf("biStats NumAdded is %s \n", biStatsJson)
	return nil
}

// add data to BulkIndexer
func addDataToBulkIndexer(object model.RedisObject, bi esutil.BulkIndexer, redisInstaceName string) {
	//format es data
	expiration := "-1"
	elemCount := 0
	if object.GetExpiration() != nil {
		expiration = object.GetExpiration().Format("2006-01-02 15:04:05")
	}
	if object.GetType() == "string" {
		elemCount = object.GetSize()
	} else {
		elemCount = object.GetElemCount()
	}

	structuredObject := model.StructuredObject{
		DBIndex:          object.GetDBIndex(),
		RedisInstaceName: redisInstaceName,
		Key:              object.GetKey(),
		Type:             object.GetType(),
		Size:             object.GetSize(),
		Byte:             bytefmt.FormatSize(uint64(object.GetSize())),
		ElemCount:        elemCount,
		Expiration:       expiration,
	}
	data, err := json.Marshal(structuredObject)

	if err != nil {
		log.Fatalf("Cannot encode  %d: %s", err)
	}
	// Add an item to the BulkIndexer
	err = bi.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "index",
			// Body is an `io.Reader` with the payload
			Body: bytes.NewReader(data),
			// OnSuccess is called for each successful operation
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				fmt.Println("add success : ", object.GetKey())
			},
			// OnFailure is called for each failed operation
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					log.Printf("ERROR: %s", err)
				} else {
					log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
				}
			},
		},
	)
}

// ToJsons read rdb file and convert to json file
func ToJsons(rdbFilename string, jsonFilename string, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if jsonFilename == "" {
		return errors.New("output file path is required")
	}
	// open file
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	jsonFile, err := os.Create(jsonFilename)
	if err != nil {
		return fmt.Errorf("create json %s failed, %v", jsonFilename, err)
	}
	defer func() {
		_ = jsonFile.Close()
	}()
	// create decoder
	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}
	// parse rdb
	_, err = jsonFile.WriteString("[\n")
	if err != nil {
		return fmt.Errorf("write json  failed, %v", err)
	}
	empty := true
	err = dec.Parse(func(object model.RedisObject) bool {
		data, err := json.Marshal(object)
		if err != nil {
			fmt.Printf("json marshal failed: %v", err)
			return true
		}
		data = append(data, ',', '\n')
		_, err = jsonFile.Write(data)
		if err != nil {
			fmt.Printf("write failed: %v", err)
			return true
		}
		empty = false
		return true
	})
	if err != nil {
		return err
	}
	// finish json
	if !empty {
		_, err = jsonFile.Seek(-2, 2)
		if err != nil {
			return fmt.Errorf("error during seek in file: %v", err)
		}
	}
	_, err = jsonFile.WriteString("\n]")
	if err != nil {
		return fmt.Errorf("error during write in file: %v", err)
	}
	return nil
}

// ToAOF read rdb file and convert to aof file (Redis Serialization )
func ToAOF(rdbFilename string, aofFilename string, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if aofFilename == "" {
		return errors.New("output file path is required")
	}
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	aofFile, err := os.Create(aofFilename)
	if err != nil {
		return fmt.Errorf("create json %s failed, %v", aofFilename, err)
	}
	defer func() {
		_ = aofFile.Close()
	}()

	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}
	return dec.Parse(func(object model.RedisObject) bool {
		cmdLines := ObjectToCmd(object)
		data := CmdLinesToResp(cmdLines)
		_, err = aofFile.Write(data)
		if err != nil {
			fmt.Printf("write failed: %v", err)
			return true
		}
		return true
	})
}
