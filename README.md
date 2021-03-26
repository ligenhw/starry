# starry

[![GitHub Actions CI][ciBadge]][ciLink]

实时流计算任务, 基于flink

* NginxAccessLogEtlJob

source : kafka

sink: es

数据流程
http-client => nginx => rsyslog => kafka => flink job => es => grafana




[ciBadge]: https://github.com/ligenhw/starry/workflows/CI/badge.svg
[ciLink]: https://github.com/ligenhw/starry/actions
