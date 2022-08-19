package cn.bestlang.starry.evt

import cn.bestlang.starry.udf.ToBigInt
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object CollectEvt1mPvJob {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val topic = params.get("kafka.topic")
    val bootStrapServers = params.get("kafka.bootstrap.servers")
    val groupId = params.get("kafka.group.id")
    val esHost = params.get("es.host")
    val esIndex = params.get("es.index")
    val esUsername = params.get("es.username")
    val esPassword = params.get("es.password")

    val settings = EnvironmentSettings.newInstance.build
    val tEnv = StreamTableEnvironment.create(env, settings)
    val configuration = tEnv.getConfig().getConfiguration()
    // https://ci.apache.org/projects/flink/flink-docs-release-1.13/zh/docs/dev/table/config/#table-exec-source-idle-timeout
    configuration.setLong("table.exec.source.idle-timeout", 10000)


    tEnv.executeSql(
      """
        | CREATE TABLE collect_evt (
        |   evt STRING,
        |   app STRING,
        |   uid STRING,
        |   ts BIGINT,
        |   url STRING,
        |   action STRING,
        |   time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
        |   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND
        | ) WITH (
        |   'connector' = 'kafka',
        |   'topic' = '%1$s',
        |   'properties.bootstrap.servers' = '%2$s',
        |   'properties.group.id' = '%3$s',
        |   'properties.auto.offset.reset' = 'earliest',
        |   'format' = 'json'
        | )
        |""".stripMargin format(topic, bootStrapServers, groupId))

    tEnv.executeSql(
      """
        | CREATE TABLE evt_1m_pv (
        |   wStart TIMESTAMP(3),
        |   app STRING,
        |   url STRING,
        |   `count` BIGINT,
        |   ts BIGINT
        | ) WITH (
        |   'connector' = 'elasticsearch-7',
        |   'hosts' = '%1$s',
        |   'username' = '%3$s',
        |   'password' = '%4$s',
        |   'index' = '%2$s-{wStart|yyyy-MM-dd}'
        | )
        |""".stripMargin format(esHost, esIndex, esUsername, esPassword)
    )

    // 注册函数
    tEnv.createTemporarySystemFunction("TO_BIGINT", classOf[ToBigInt])

    tEnv.executeSql(
      """
        |INSERT INTO evt_1m_pv
        |   SELECT
        |     TUMBLE_START(time_ltz, INTERVAL '1' MINUTE) AS wStart,
        |     app,
        |     url,
        |     COUNT(*) AS `count`,
        |     TO_BIGINT(TUMBLE_START(time_ltz, INTERVAL '1' MINUTE)) AS ts
        |   FROM collect_evt
        |     WHERE evt = 'pv'
        |     GROUP BY TUMBLE(time_ltz, INTERVAL '1' MINUTE), app, url
        |""".stripMargin).await()
  }
}
