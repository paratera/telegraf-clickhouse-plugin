package clickhouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/influxdata/telegraf"
	"github.com/juju/errors"
	"github.com/ClickHouse/clickhouse-go"
)

type ClickhouseClient struct {

	// DBI example: tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000

	DBI          string
	Addr         string `toml:"addr"`
	Port         int64  `toml:"port"`
	User         string `toml:"user"`
	Password     string `toml:"password"`
	Database     string `toml:"database"`
	TableName    string `toml:"tablename"`
	WriteTimeout int64  `toml:"write_timeout"`

	Debug bool `toml:"debug"`

	db *sql.DB
}

func newClickhouse() *ClickhouseClient {
	return &ClickhouseClient{}
}

func (c *ClickhouseClient) Connect() error {
	var err error

	c.DBI = fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&write_timeout=%d&debug=%t",
		c.Addr,
		c.Port,
		c.User,
		c.Password,
		c.WriteTimeout,
		c.Debug,
	)

	if c.Debug {
		log.Println("DBI=", c.DBI)
	}

	c.db, err = sql.Open("clickhouse", c.DBI)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *ClickhouseClient) Close() error {
	return nil
}

func (c *ClickhouseClient) Description() string {
	return "Telegraf Output Plugin for Clickhouse"
}

func (c *ClickhouseClient) SampleConfig() string {
	return `
Schema:
> CREATE TABLE telegraf.metrics(
	date Date DEFAULT toDate(ts),
	name String,
	tags String,
	val Float64,
	ts DateTime,
	updated DateTime DEFAULT now()
) ENGINE=MergeTree(date,(name,tags,ts),8192)

telegraf.conf
[[outputs.clickhouse]]
    user = "default"
    password = ""
    addr = 127.0.0.1
    port = 9000
    database = "telegraf"
	tablename = "metrics"
	write_timeout = 10
	debug = true
`
}

func (c *ClickhouseClient) Write(metrics []telegraf.Metric) (err error) {
	err = nil
	var batchMetrics []clickhouseMetrics

	if c.Debug {
		log.Println("Recv Telegraf Metrics:", metrics)
	}

	for _, metric := range metrics {
		var tmpClickhouseMetrics clickhouseMetrics

		tmpClickhouseMetrics = *newClickhouseMetrics(metric)

		batchMetrics = append(batchMetrics, tmpClickhouseMetrics)
	}

	if c.Debug {
		log.Println("Replace Metrics to Clickhouse Format ", batchMetrics)
	}

	if err = c.db.Ping(); err != nil {
		if c.Debug {
			if exception, ok := err.(*clickhouse.Exception); ok {
				log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			} else {
				log.Println(err)
			}
		}
		return errors.Trace(err)
	}

	// create database
	stmtCreateDatabase := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.Database)
	if c.Debug {
		log.Println("Create Database: ", stmtCreateDatabase)
	}
	_, err = c.db.Exec(stmtCreateDatabase)
	if err != nil {
		if c.Debug {
			log.Println(err.Error())
		}
		return errors.Trace(err)
	}

	// create table
	stmtCreateTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.%s(
		date Date DEFAULT toDate(ts),
		name String,
		tags String,
		val Float64,
		ts DateTime,
		updated DateTime DEFAULT now()
	) ENGINE=MergeTree(date,(name,tags,ts),8192)
	`, c.Database, c.TableName)

	if c.Debug {
		log.Println("Create Table :", stmtCreateTable)
	}
	_, err = c.db.Exec(stmtCreateTable)
	if err != nil {
		if c.Debug {
			log.Fatal(err.Error())
		}
		return errors.Trace(err)
	}

	// start transaction
	Tx, err := c.db.Begin()
	if c.Debug {
		log.Println("Starting Transaction.")
	}
	if err != nil {
		if c.Debug {
			log.Fatal(err.Error())
		}
		return errors.Trace(err)
	}

	// Prepare stmt
	stmtInsertData := fmt.Sprintf("INSERT INTO %s.%s(name,tags,val,ts) VALUES(?,?,?,?)", c.Database, c.TableName)
	Stmt, err := Tx.Prepare(stmtInsertData)
	if err != nil {
		if c.Debug {
			log.Println(err.Error())
		}
		return errors.Trace(err)
	}
	defer Stmt.Close()

	for _, metrs := range batchMetrics {
		for _, metr := range metrs {
			tags, _ := json.Marshal(metr.Tags)
			if c.Debug {
				log.Println(
					"Name:", metr.Name,
					"Tags:", string(tags),
					"Val:", metr.Val,
					"Ts:", metr.Ts,
				)
			}
			if _, err := Stmt.Exec(
				metr.Name,
				string(tags),
				metr.Val,
				metr.Ts,
			); err != nil {
				if c.Debug {
					fmt.Println(err.Error())
				}
			}
		}
	}

	// commit transaction.
	if err := Tx.Commit(); err != nil {
		return errors.Trace(err)
	}

	if c.Debug {
		log.Println("Transaction Commit")
	}

	return err
}
