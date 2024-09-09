/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/


package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	cdc "github.com/wesql/wescale-cdc"
	"github.com/wesql/wescale/examples/cdc/rediscdc"
	"strconv"
	"testing"
	"time"
)

var (
	host             = "127.0.0.1"
	port             = 15306
	tableSchema      = "d1"
	tableName        = "redis_test"
	createTableQuery = fmt.Sprintf("create table if not exists %s (id int primary key, name varchar(256))", tableName)

	RedisAddr = "127.0.0.1:6379"
	RedisPWD  = ""
	RedisDB   = "0"
)

func mockConfig() {
	cdc.DefaultConfig.TableSchema = tableSchema
	cdc.DefaultConfig.SourceTableName = tableName
	cdc.DefaultConfig.FilterStatement = fmt.Sprintf("select * from %s", tableName)
	cdc.DefaultConfig.WeScaleHost = "127.0.0.1"
	cdc.DefaultConfig.WeScaleGrpcPort = "15991"

	rediscdc.RedisAddr = "127.0.0.1:6379"
	rediscdc.RedisPWD = ""
	rediscdc.RedisDB = "0"
}

func TestBasic(t *testing.T) {
	dsn := fmt.Sprintf("(%s:%d)/%s", host, port, tableSchema)
	db, err := sql.Open("mysql", dsn)
	assert.Nil(t, err)
	defer db.Close()

	// drop and create table
	_, err = db.Exec(createTableQuery)
	assert.Nil(t, err)

	// insert init data
	_, err = db.Exec(fmt.Sprintf("insert ignore into %s values (1, 'a')", tableName))
	assert.Nil(t, err)

	// start cdc
	mockConfig()
	cc := cdc.NewCdcConsumer()
	cc.Open()
	defer cc.Close()
	go cc.Run()

	// init redis client
	redisDB, err := strconv.Atoi(RedisDB)
	assert.Nil(t, err)
	opts := &redis.Options{
		Addr:     RedisAddr,
		Password: RedisPWD,
		DB:       redisDB,
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()

	// query data in redis
	pkKey1, err := rediscdc.GenerateRedisKey(rediscdc.TableData, "1")
	assert.Nil(t, err)

	value, err := waitForData(rdb, pkKey1, true, 30*time.Second)
	assert.Nil(t, err)
	data := make(map[string]any)
	err = json.Unmarshal([]byte(value), &data)
	assert.Nil(t, err)
	assert.Equal(t, "a", data["name"])

	// insert new data in mysql, it should be found in redis
	_, err = db.Exec(fmt.Sprintf("insert into %s values (2, 'b')", tableName))
	assert.Nil(t, err)
	pkKey2, err := rediscdc.GenerateRedisKey(rediscdc.TableData, "2")
	assert.Nil(t, err)
	value, err = waitForData(rdb, pkKey2, true, 30*time.Second)
	assert.Nil(t, err)
	data = make(map[string]any)
	err = json.Unmarshal([]byte(value), &data)
	assert.Nil(t, err)
	assert.Equal(t, "b", data["name"])

	// update data in mysql, it should be update in redis
	_, err = db.Exec(fmt.Sprintf("update %s set id = 3, name ='c' where id = 2", tableName))
	assert.Nil(t, err)
	pkKey3, err := rediscdc.GenerateRedisKey(rediscdc.TableData, "3")
	assert.Nil(t, err)
	value, err = waitForData(rdb, pkKey3, true, 30*time.Second)
	assert.Nil(t, err)
	_, err = waitForData(rdb, pkKey2, false, 30*time.Second)
	assert.Nil(t, err)
	data = make(map[string]any)
	err = json.Unmarshal([]byte(value), &data)
	assert.Nil(t, err)
	assert.Equal(t, "c", data["name"])

	// delete data in mysql, it should be deleted in redis
	_, err = db.Exec(fmt.Sprintf("delete from %s ", tableName))
	assert.Nil(t, err)
	_, err = waitForData(rdb, pkKey3, false, 30*time.Second)
	assert.Nil(t, err)
	_, err = waitForData(rdb, pkKey1, false, 30*time.Second)
	assert.Nil(t, err)

	// clean meta in redis
	metaKey, err := rediscdc.GenerateRedisKey(rediscdc.Meta, "")
	assert.Nil(t, err)
	_, err = rdb.Del(context.Background(), metaKey).Result()
	assert.Nil(t, err)
}

func waitForData(RDBClient *redis.Client, key string, expectExist bool, timeout time.Duration) (string, error) {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		val, err := RDBClient.Get(ctx, key).Result()

		if err == redis.Nil {
			if !expectExist {
				return "", nil
			}
		} else if err != nil {
			return "", fmt.Errorf("error querying redis: %v", err)
		} else {
			// key exists
			if expectExist {
				return val, nil
			}
		}

		// retry
		time.Sleep(100 * time.Millisecond)
	}

	if expectExist {
		return "", fmt.Errorf("timeout: expected key to exist but it did not within %v", timeout)
	} else {
		return "", fmt.Errorf("timeout: expected key to not exist but it did within %v", timeout)
	}
}
