package cn.bestlang.starry.job

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.junit.jupiter.api.Test

class NginxAccessLogEtlToEsJobTest {

  @Test
  def main(): Unit = {

    val settings = EnvironmentSettings.newInstance.build
    val tEnv = TableEnvironment.create(settings)

    tEnv.executeSql("""
      CREATE TABLE nginx_log (
        log STRING
      ) WITH (
        'connector' = 'filesystem',
        'path' = 'src/test/resources/nginx-access.log',
        'format' = 'raw'
      )
      """.stripMargin
    )

    tEnv.executeSql(
      """
        |CREATE TABLE parse_result (
        | remote_addr STRING,
        | remote_user STRING,
        | time_local BIGINT,
        | request STRING,
        | status STRING,
        | body_bytes_sent STRING,
        | http_referer STRING,
        | http_user_agent STRING,
        | http_x_forwarded_for STRING
        |) WITH (
        | 'connector' = 'filesystem',
        | 'path' = '/tmp/nginx-access-log-result',
        | 'format' = 'json'
        |)
        |""".stripMargin)

    // 注册函数
    tEnv.createTemporarySystemFunction("AccessLogParser", classOf[AccessLogParser])

//    tEnv.executeSql(
//      """
//        |SELECT AccessLogParser(log) as t FROM nginx_log
//        |""".stripMargin).print()

    // 没有 await 会导致直接退出 没产生文件
    tEnv.executeSql(
      """
        |INSERT INTO parse_result
        |  SELECT t.* FROM (
        |    SELECT AccessLogParser(log) as t FROM nginx_log
        |  )
        |""".stripMargin).await()

  }

}
