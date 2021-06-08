# starry

[![GitHub Actions CI][ciBadge]][ciLink]

实时流计算任务, 基于flink

数据流程
http-client => nginx => rsyslog => kafka => flink job => es => grafana

## 任务列表

* NginxAccessLogEtlToEsJob

解析 nginx access log，写入es

source : kafka
sink: es

* NginxAccessLogCollectEvtEtlToKafkaJob

解析 nginx access log
解析 request 中的query参数
写入kafka

source : kafka
sink: kafka

* CollectEvtEtlToEsJob

读取 collect-evt kafka
写入es

source : kafka
sink: es






[ciBadge]: https://github.com/ligenhw/starry/workflows/CI/badge.svg
[ciLink]: https://github.com/ligenhw/starry/actions
