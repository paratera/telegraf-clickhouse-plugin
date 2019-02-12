package clickhouse

import (
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	clickhouse "github.com/roistat/go-clickhouse"
)

type ClickhouseClient struct {

	// DBI example: tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000

	DBI       string
	Addr      string
	Port      int64
	User      string
	Password  string
	Database  string
	TableName string

	Hosts []string

	TimeShift int64 `toml:"time_shift"`

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	connection *clickhouse.Conn
}

// new Clickhouse client
func newClickhouse() *ClickhouseClient {
	ch := new(ClickhouseClient)

	ch.Addr = "127.0.0.1"
	ch.Port = 9000
	ch.User = ""
	ch.Password = ""
	ch.Database = "telegraf"
	ch.TableName = "metrics"
	ch.Hosts = []string{}

	ch.ReadTimeout = time.Second * 10
	ch.WriteTimeout = time.Second * 20

	return ch
}

// set Clickhouse tcp address.
func (ch *ClickhouseClient) SetAddr(addr string) {
	ch.Addr = addr
}

// set Clickhouse tcp port.
func (ch *ClickhouseClient) SetPort(port int64) {
	ch.Port = port
}

// set Clickhouse username.
func (ch *ClickhouseClient) SetUser(user string) {
	ch.User = user
}

// set Clickhouse user password.
func (ch *ClickhouseClient) SetPassword(pass string) {
	ch.Password = pass
}

// set Clickhouse database name.
func (ch *ClickhouseClient) SetDatabase(db string) {
	ch.Database = db
}

// set Clickhouse table name.
func (ch *ClickhouseClient) SetTableName(tbname string) {
	ch.TableName = tbname
}

// set Clichouse all hosts .
func (ch *ClickhouseClient) SetHosts(hosts ...string) {
	for _, v := range hosts {
		ch.Hosts = append(ch.Hosts, v)
	}
}

// set Clickhouse Database Interface.
func (ch *ClickhouseClient) SetDBI() {
	ch.DBI = fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s&read_timeout=%d&write_timeout=%d&alt_hosts=%s",
		ch.Addr,
		ch.Port,
		ch.User,
		ch.Password,
		int(ch.ReadTimeout),
		int(ch.WriteTimeout),
		strings.Join(ch.Hosts, ","),
	)
}

func (c *ClickhouseClient) Connect() error {
	transport := clickhouse.NewHttpTransport()
	transport.Timeout = c.timeout

	c.connection = clickhouse.NewConn(c.URL, transport)

	err := c.connection.Ping()
	if err != nil {
		return err
	}

	for _, create_sql := range c.SQLs {
		query := clickhouse.NewQuery(create_sql)
		err = query.Exec(c.connection)
		if err != nil {
			return err
		}
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
	inserts := make(map[string]*clickhouseMetrics)

	for _, metric := range metrics {
		//table := c.Database + "." + metric.Name()
		table := c.Database + "." + c.TableName

		if _, exists := inserts[table]; !exists {
			inserts[table] = &clickhouseMetrics{}
		}

		inserts[table].AddMetric(metric, c.TimeShift)
	}

	for table, insert := range inserts {
		if len(*insert) == 0 {
			continue
		}

		columns := insert.GetColumns()
		rows := insert.GetRowsByColumns(columns)

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
	return err
}
