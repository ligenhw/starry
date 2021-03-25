package cn.bestlang.starry.job

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object NginxAccessLogEtlJob {

  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance.build
    val tEnv = TableEnvironment.create(settings)

    tEnv.executeSql("""
      CREATE TABLE nginx_log (
        log STRING
      ) WITH (
        'connector' = 'kafka',
        'topic' = 'nginx-collect-access-log',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'format' = 'raw'
      )
      """.stripMargin
    )

    // 注册函数
    tEnv.createTemporarySystemFunction("AccessLogParser", classOf[AccessLogParser])

    tEnv.executeSql("""
      CREATE TABLE nginx_log_index (
        remote_addr STRING,
        remote_user STRING,
        time_local BIGINT,
        request STRING,
        status STRING,
        body_bytes_sent STRING,
        http_referer STRING,
        http_user_agent STRING,
        http_x_forwarded_for STRING
      ) WITH (
        'connector' = 'elasticsearch-7',
        'hosts' = 'http://localhost:9200',
        'index' = 'nginx_log_index'
      )
    """.stripMargin
    )

    // 运行一个 INSERT 语句，将源表的数据输出到结果表中
    val tableResult = tEnv.executeSql(
      "INSERT INTO nginx_log_index SELECT t.* FROM (SELECT AccessLogParser(log) as t FROM nginx_log)")
    // 通过 TableResult 来获取作业状态
    println(tableResult.getJobClient().get().getJobStatus())
  }


}
