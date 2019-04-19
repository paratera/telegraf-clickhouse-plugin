package clickhouse

import (
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
)

type (
	// a metric of clickhouse
	clickhouseMetric struct {
		Date    string                 `json:"date" db:"date"`
		Name    string                 `json:"name" db:"name"`
		Tags    map[string]interface{} `json:"tags" db:"tags"`
		Val     float64                `json:"val" db:"val"`
		Ts      time.Time              `json:"ts" db:"ts"`
		Updated time.Time              `json:"updated" db:"updated"`
	}

	// metrics of clickhouse
	clickhouseMetrics []clickhouseMetric
)

func newClickhouseMetrics(metric telegraf.Metric) *clickhouseMetrics {
	//var fieldCount int
	cm := new(clickhouseMetrics)

	//fieldCount = len(metric.FieldList())

	for _, field := range metric.FieldList() {
		// tmp variables
		var tmpClickhouseMetric clickhouseMetric
		tags := make(map[string]interface{})
		var tmpCurrentTime time.Time

		tmpCurrentTime = time.Now()

		if strings.Compare(field.Key, "gauge") == 0 {
			tmpClickhouseMetric.Name = fmt.Sprintf("%s", metric.Name())
		} else {
			tmpClickhouseMetric.Name = fmt.Sprintf("%s_%s", metric.Name(), field.Key)
		}

		tmpFiledValue := convertField(field.Value)
		if tmpFiledValue == nil {
			tags[field.Key] = field.Value.(string)
			for _, value := range metric.TagList() {
				tags[value.Key] = value.Value
			}

			tmpClickhouseMetric.Tags = tags
			tmpClickhouseMetric.Val = float64(0)
			tmpClickhouseMetric.Ts = metric.Time()
			tmpClickhouseMetric.Updated = tmpCurrentTime

		} else {

			for _, value := range metric.TagList() {
				tags[value.Key] = value.Value
			}

			tmpClickhouseMetric.Val = tmpFiledValue.(float64)
			tmpClickhouseMetric.Tags = tags
			tmpClickhouseMetric.Ts = metric.Time()
			tmpClickhouseMetric.Updated = tmpCurrentTime

		}

		*cm = append(*cm, tmpClickhouseMetric)
	}
	return cm
}

// convert field to a supported type or nil if unconvertible
func convertField(v interface{}) interface{} {
	switch v := v.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case bool:
		if v {
			return float64(1)
		} else {
			return float64(0)
		}
	case int:
		return float64(v)
	case uint:
		return float64(v)
	case uint64:
		return float64(v)
	case int32:
		return float64(v)
	case int16:
		return float64(v)
	case int8:
		return float64(v)
	case uint32:
		return float64(v)
	case uint16:
		return float64(v)
	case uint8:
		return float64(v)
	case float32:
		return float64(v)
	default:
		return nil
	}
}
