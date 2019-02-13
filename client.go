package clickhouse

import (
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/juju/errors"
	"github.com/kshvakov/clickhouse"
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

	db clickhouse.Clickhouse
}

func newClickhouse() *ClickhouseClient {
	return &ClickhouseClient{}
}

func (c *ClickhouseClient) Connect() error {
	var err error

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
	c.db, err = clickhouse.OpenDirect(c.DBI)
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

	for _, metric := range metrics {
		//table := c.Database + "." + metric.Name()
		//table := c.database + "." + c.tableName

		fmt.Println(newClickhouseMetrics(metric))
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
	return err
}
