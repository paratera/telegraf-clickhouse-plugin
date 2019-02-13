package clickhouse

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
)

// register plugin.
func init() {
	outputs.Add("clickhouse", func() telegraf.Output {
		return newClickhouse()
	})
}
