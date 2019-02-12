package clickhouse

import (
	"github.com/influxdb/telegraf"
	"github.com/influxdb/telegraf/plugins/outputs"
)

// register plugin.
func init() {
	outputs.Add("clickhouse", func() telegraf.Output {
		return newClickhouse()
	})
}
