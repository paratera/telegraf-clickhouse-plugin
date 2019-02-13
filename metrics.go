package clickhouse

import (
	"fmt"
	"time"

	"github.com/influxdata/telegraf"
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

func newClickhouseMetrics(metric telegraf.Metric) *clickhouseMetrics {
	//var fieldCount int
	cm := new(clickhouseMetrics)

	//fieldCount = len(metric.FieldList())

	for _, field := range metric.FieldList() {
		// tmp variables
		var tmpClickhouseMetric clickhouseMetric
		var tmpTags string
		var tmpFieldTag string

		tmpClickhouseMetric.Name = fmt.Sprint("%s_%s", metric.Name(), field.Key)

		tmpFiledValue := convertField(field.Value)
		if tmpFiledValue == nil {
			tmpFieldTag = field.Value.(string)
			break
		}

		tmpClickhouseMetric.Tags = append(tmpClickhouseMetric.Tags, tmpFieldTag)

		for _, value := range metric.TagList() {
			tmpTags = fmt.Sprintf("%s=%s", value.Key, value.Value)
			tmpClickhouseMetric.Tags = append(tmpClickhouseMetric.Tags, tmpTags)
		}

		tmpClickhouseMetric.Val = tmpFiledValue.(float64)

		tmpClickhouseMetric.Ts = metric.Time()
		//tmpClickhouseMetric.Value

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
