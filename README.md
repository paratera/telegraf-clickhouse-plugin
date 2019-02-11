# Telegraf output plugin for sending metrics to clickhouse
-----

## 1. How to use

## 1.1. add plugin files to telegraf repository.

```bash
# mkdir -p telegraf/plugins/outputs/clickhouse
# cp clickhouse.go telegraf/plugins/outputs/clickhouse
```

### 1.2. Enable this plugin

Append plugin into plugins/outpus/all/all.go

```bash
        _ "github.com/influxdata/telegraf/plugins/outputs/socket_writer"
        _ "github.com/influxdata/telegraf/plugins/outputs/stackdriver"
        _ "github.com/influxdata/telegraf/plugins/outputs/wavefront"
+       _ "github.com/influxdata/telegraf/plugins/outputs/clickhouse"
)
```

### 1.2. build telegraf

```bash
# cd telegraf
# make
```

### 2. Donate

-----

If you like the project and want to buy me a cola, you can through:

| PayPal                                                                                                               | 微信                                                                 |
| -------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| [![](https://www.paypalobjects.com/webstatic/paypalme/images/pp_logo_small.png)](https://www.paypal.me/taylor840326) | ![](https://github.com/taylor840326/blog/raw/master/imgs/weixin.png) |
