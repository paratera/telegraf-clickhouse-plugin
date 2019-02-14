package clickhouse

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/juju/errors"
	"github.com/kshvakov/clickhouse"
	"github.com/kshvakov/clickhouse/lib/data"
)

type ClickhouseClient struct {

	// DBI example: tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000

	DBI          string
	Addr         string `toml:"addr"`
	Port         int64  `toml:"port"`
	User         string `toml:"user"`
	Database     string `toml:"database"`
	TableName    string `toml:"tablename"`
	IsCompressed string `toml:"compress"`

	readTimeout  time.Duration
	writeTimeout time.Duration

	db *sql.DB
}

func newClickhouse() *ClickhouseClient {
	return &ClickhouseClient{}
}

func (c *ClickhouseClient) Connect() error {
	var err error
	var compress string

	switch c.IsCompressed {
	case "0":
		compress = "false"
	case "1":
		compress = "true"
	case "on":
		compress = "true"
	case "off":
		compress = "false"
	default:
		compress = "false"
	}
	c.DBI = fmt.Sprintf("tcp://%s:%d?username=%s&compress=%s&debug=true",
		c.Addr,
		c.Port,
		c.User,
		compress,
	)

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
	tags Array(String),
	val Float64,
	ts DateTime,
	updated DateTime DEFAULT now()
) ENGINE=MergeTree(date,(name,tags,ts),8192)
addr = 127.0.0.1
port = 9000
user = ""
database = ""
tablename = ""
compress = 0
`
}

func (c *ClickhouseClient) Write(metrics []telegraf.Metric) (err error) {
	err = nil
	var batchMetrics []clickhouseMetrics

	for _, metric := range metrics {
		var tmpClickhouseMetrics clickhouseMetrics

		tmpClickhouseMetrics = *newClickhouseMetrics(metric)

		batchMetrics = append(batchMetrics, tmpClickhouseMetrics)
	}

	if err = c.db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			log.Println(err)
		}
		return errors.Trace(err)
	}

	// create database
	stmtCreateDatabase := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.Database)
	_, err = c.db.Exec(stmtCreateDatabase)
	if err != nil {
		log.Fatal(err)
		return errors.Trace(err)
	}

	// create table
	stmtCreateTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.%s(
		date Date DEFAULT toDate(ts),
		name String,
		tags Array(String),
		val Float64,
		ts DateTime,
		updated DateTime DEFAULT now()
	) ENGINE=MergeTree(date,(name,tags,ts),8192)
	`, c.Database, c.TableName)
	_, err = c.db.Exec(stmtCreateTable)
	if err != nil {
		log.Fatal(err)
		return errors.Trace(err)
	}

	// start transaction
	Tx, err := c.db.Begin()
	if err != nil {
		log.Fatal(err)
		return errors.Trace(err)
	}

	// Prepare stmt
	stmtInsertData := fmt.Sprintf("INSERT INTO %s.%s(name,tags,val,ts,updated) VALUES(?,?,?,?,?)", c.Database, c.TableName)
	Stmt, err := Tx.Prepare(stmtInsertData)
	if err != nil {
		log.Println(stmtInsertData)
		log.Println(err)
		return errors.Trace(err)
	}
	defer Stmt.Close()

	//var wg sync.WaitGroup
	for _, metrs := range batchMetrics {
		//wg.Add(1)
		//go func() {
		//defer wg.Done()
		for _, metr := range metrs {
			if _, err := Stmt.Exec(
				metr.Name,
				clickhouse.Array(metr.Tags),
				metr.Val,
				metr.Ts,
				metr.Updated,
			); err != nil {
				fmt.Println(err.Error())
			}
		}
		//}()
	}

	//wg.Wait()

	// commit transaction.
	if err := Tx.Commit(); err != nil {
		return errors.Trace(err)
	}

	return err
}

// batch write
func writeBatch(block *data.Block, metrics clickhouseMetrics, count int) {
	block.Reserve()
	block.NumRows += uint64(count)

	for row := 0; row < count; row++ {
		block.WriteString(0, metrics[row].Date)
	}

	for row := 0; row < count; row++ {
		block.WriteString(1, metrics[row].Name)
	}
	for row := 0; row < count; row++ {
		block.WriteArray(2, clickhouse.Array(metrics[row].Tags))
	}
	for row := 0; row < count; row++ {
		block.WriteFloat64(3, metrics[row].Val)
	}
	for row := 0; row < count; row++ {
		block.WriteDateTime(4, metrics[row].Ts)
	}
	for row := 0; row < count; row++ {
		block.WriteDateTime(5, metrics[row].Updated)
	}
}
