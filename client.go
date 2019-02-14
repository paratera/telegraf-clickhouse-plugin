package clickhouse

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/juju/errors"
	"github.com/kshvakov/clickhouse"
	"github.com/kshvakov/clickhouse/lib/data"
)

type ClickhouseClient struct {

	// DBI example: tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000

	DBI          string
	Addr         string   `toml:"addr"`
	Port         int64    `toml:"port"`
	User         string   `toml:"user"`
	Password     string   `toml:"password"`
	Database     string   `toml:"database"`
	TableName    string   `toml:"tablename"`
	IsCompressed int      `toml:"compress"`
	Hosts        []string `toml:"hosts"`

	readTimeout  time.Duration
	writeTimeout time.Duration

	db *sql.DB
}

func newClickhouse() *ClickhouseClient {
	return &ClickhouseClient{}
}

func (c *ClickhouseClient) Connect() error {
	var err error

	/*
		c.DBI = fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s&read_timeout=%d&write_timeout=%d&alt_hosts=%s&compress=%d",
			c.Addr,
			c.Port,
			c.User,
			c.Password,
			c.Database,
			10,
			20,
			strings.Join(c.Hosts, ","),
			0,
		)
	*/

	c.db, err = sql.Open("clickhouse", "tcp://172.18.10.100:9000?username=&debug=true&compress=0")
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
password = ""
database = ""
tablename = ""
compress = 0
hosts = ["127.0.0.1:9001","127.0.0.1:9002"]
`
}

func (c *ClickhouseClient) Write(metrics []telegraf.Metric) (err error) {
	err = nil
	var batchMetrics []clickhouseMetrics

	//telegrafMetricsLen := len(metrics)

	for _, metric := range metrics {
		//table := c.Database + "." + metric.Name()
		//table := c.database + "." + c.tableName
		var tmpClickhouseMetrics clickhouseMetrics

		tmpClickhouseMetrics = *newClickhouseMetrics(metric)

		batchMetrics = append(batchMetrics, tmpClickhouseMetrics)
	}
	/*
		for table, insert := range inserts {
			if len(*insert) == 0 {
				continue
			}

			var query clickhouse.Query
			query, err = clickhouse.BuildMultiInsert(table, columns, rows)
			if err != nil {
				continue
			}

			err = query.Exec(c.connection)
			if err != nil {
				continue
			}

		}
	*/
	//fmt.Println(batchMetrics)

	if err := c.db.Ping(); err != nil {
		return errors.Trace(err)
	}

	Tx, err := c.db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	stmt := fmt.Sprintf("INSERT INTO %s.%s(name,tags,val,ts,updated) VALUES(?,?,?,?,?)", c.Database, c.TableName)
	Stmt, err := Tx.Prepare(stmt)
	if err != nil {
		return errors.Trace(err)
	}
	defer Stmt.Close()

	//MetricsCount := telegrafMetricsLen * clickhouseMetricLen
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
