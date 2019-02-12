package clickhouse

import (
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/roistat/go-clickhouse"
)

type (
	// a metric of clickhouse
	clickhouseMetric struct {
		Date    string    `json:"date" db:"date"`
		Name    string    `json:"name" db:"name"`
		Tags    []string  `json:"tags" db:"tags"`
		Val     float64   `json:"val" db:"val"`
		Ts      time.Time `json:"ts" db:"ts"`
		Updated time.Time `json:"updated" db:"updated"`
	}

	// metrics of clickhouse
	clickhouseMetrics []clickhouseMetric
)

func newClickhouseMetric(metric telegraf.Metric, timeShift int64) *clickhouseMetric {
	cm := &clickhouseMetric{}

	for name, value := range metric.Fields() {
		cm.AddData(name, value, true)
	}
	for name, value := range metric.Tags() {
		cm.AddData(name, value, true)
	}

	metricTime := metric.Time().Add(time.Duration(timeShift))
	date := metricTime.Format("2006-01-02")
	datetime := metricTime.Format("2006-01-02 15:04:05")
	cm.AddData("date", date, true)
	cm.AddData("datetime", datetime, true)

	return cm
}
func (cm *clickhouseMetric) GetColumns() []string {
	columns := make([]string, 0)

	for column := range *cm {
		columns = append(columns, column)
	}
	return columns
}
func (cm *clickhouseMetric) AddData(name string, value interface{}, overwrite bool) {
	if _, exists := (*cm)[name]; !overwrite && exists {
		return
	}

	(*cm)[name] = value
}

type clickhouseMetrics []*clickhouseMetric

func (cms *clickhouseMetrics) GetColumns() []string {
	if len(*cms) == 0 {
		return []string{}
	}

	randomMetric := (*cms)[0] // all previous metrics are same
	return randomMetric.GetColumns()
}
func (cms *clickhouseMetrics) AddMissingColumn(name string, value interface{}) {
	for _, metric := range *cms {
		metric.AddData(name, value, false)
	}
}
func (cms *clickhouseMetrics) AddMetric(metric telegraf.Metric, timeShift int64) {
	newMetric := newClickhouseMetric(metric, timeShift)

	if len(*cms) > 0 {
		randomMetric := (*cms)[0] // all previous metrics are same

		for name := range *newMetric {
			if _, exists := (*randomMetric)[name]; !exists {
				cms.AddMissingColumn(name, 0)
			}
		}

		for name := range *randomMetric {
			if _, exists := (*newMetric)[name]; !exists {
				newMetric.AddData(name, 0, false)
			}
		}
	}

	*cms = append(*cms, newMetric)
}
func (cms *clickhouseMetrics) GetRowsByColumns(columns []string) clickhouse.Rows {
	rows := make(clickhouse.Rows, 0)

	for _, metric := range *cms {
		row := make(clickhouse.Row, 0)
		for _, column := range columns {
			row = append(row, (*metric)[column])
		}
		rows = append(rows, row)
	}

	return rows
}

// register plugin.
func init() {
	outputs.Add("clickhouse", func() telegraf.Output {
		return newClickhouse()
	})
}
