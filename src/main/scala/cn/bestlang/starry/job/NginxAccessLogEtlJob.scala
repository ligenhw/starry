package cn.bestlang.starry.job

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.functions.ScalarFunction

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
    tEnv.createTemporarySystemFunction("AccessLogSplit", classOf[AccessLogSplit])

    tEnv.executeSql("""
      SELECT AccessLogSplit(log) FROM nginx_log
    """.stripMargin
    ).print()


  }

  class AccessLogSplit extends ScalarFunction {
    def eval(s: String) = {
      s.split(" ")
    }
  }

}
