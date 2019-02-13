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

	dbi       string
	addr      string
	port      int64
	user      string
	password  string
	database  string
	tableName string

	hosts []string

	timeShift int64 `toml:"time_shift"`

	readTimeout  time.Duration
	writeTimeout time.Duration

	db clickhouse.Clickhouse
}

// new Clickhouse client
func newClickhouse() *ClickhouseClient {
	ch := new(ClickhouseClient)

	ch.addr = "127.0.0.1"
	ch.port = 9000
	ch.user = ""
	ch.password = ""
	ch.database = "telegraf"
	ch.tableName = "metrics"
	ch.hosts = []string{}

	ch.readTimeout = time.Second * 10
	ch.writeTimeout = time.Second * 20

	return ch
}

// set Clickhouse tcp address.
func (ch *ClickhouseClient) SetAddr(addr string) {
	ch.addr = addr
}

// set Clickhouse tcp port.
func (ch *ClickhouseClient) SetPort(port int64) {
	ch.port = port
}

// set Clickhouse username.
func (ch *ClickhouseClient) SetUser(user string) {
	ch.user = user
}

// set Clickhouse user password.
func (ch *ClickhouseClient) SetPassword(pass string) {
	ch.password = pass
}

// set Clickhouse database name.
func (ch *ClickhouseClient) SetDatabase(db string) {
	ch.database = db
}

// set Clickhouse table name.
func (ch *ClickhouseClient) SetTableName(tbname string) {
	ch.tableName = tbname
}

// set Clichouse all hosts .
func (ch *ClickhouseClient) SetHosts(hosts ...string) {
	for _, v := range hosts {
		ch.hosts = append(ch.hosts, v)
	}
}

// set Clickhouse Database Interface.
func (ch *ClickhouseClient) SetDBI() {
	ch.dbi = fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s&read_timeout=%d&write_timeout=%d&alt_hosts=%s",
		ch.addr,
		ch.port,
		ch.user,
		ch.password,
		int(ch.readTimeout),
		int(ch.writeTimeout),
		strings.Join(ch.hosts, ","),
	)
}

func (c *ClickhouseClient) Connect() error {
	var err error
	c.db, err = clickhouse.OpenDirect(c.dbi)
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
	date Date DEFAULT toDate(0),
	name String,
	tags Array(String),
	val Float64,
	ts DateTime,
	updated DateTime DEFAULT now()
) ENGINE=MergeTree(date,(name,tags,ts),8192)
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
