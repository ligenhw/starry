package cn.bestlang.starry.job

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.junit.jupiter.api.Test

class NginxAccessLogEtlJobTest {

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

    // 注册函数
    tEnv.createTemporarySystemFunction("AccessLogParser", classOf[AccessLogParser])

    tEnv.executeSql(
      """
        |SELECT AccessLogParser(log) as t FROM nginx_log
        |""".stripMargin).print()

  }

}
