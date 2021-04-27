package cn.bestlang.starry.evt

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.junit.jupiter.api.Test

class EvtEtlJobTest {

  @Test
  def testMain(): Unit = {
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner().inStreamingMode().build
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.executeSql(
      """
        | CREATE TABLE parsed_nginx_log (
        |   remote_addr STRING,
        |   remote_user STRING,
        |   time_local BIGINT,
        |   request STRING,
        |   status STRING,
        |   body_bytes_sent STRING,
        |   http_referer STRING,
        |   http_user_agent STRING,
        |   http_x_forwarded_for STRING
        | ) WITH (
        |   'connector' = 'filesystem',
        |   'path' = 'src/test/resources/nginx-parsed.log',
        |   'format' = 'json'
        | )
        |""".stripMargin).getTableSchema

    tEnv.executeSql(
      """
        | CREATE TABLE evt_log_index (
        |   evt STRING,
        |   app STRING,
        |   uid STRING,
        |   ts STRING,
        |   url STRING,
        |   action STRING
        | ) WITH (
        |   'connector' = 'filesystem',
        |   'path' = '/tmp/evt_log_index',
        |   'format' = 'json'
        | )
        |""".stripMargin)

    // 注册函数
    tEnv.createTemporarySystemFunction("EvtParser", classOf[EvtParser])

    tEnv.executeSql(
      """
        | INSERT INTO evt_log_index
        | SELECT t.* FROM (
        |   SELECT EvtParser(request) as t FROM parsed_nginx_log
        |   )
        |""".stripMargin).await()

  }

}
