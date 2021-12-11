package cn.bestlang.starry.evt

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object CollectEvtEtlToEsJob {

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

    val checkpointingEnabled = params.has("checkpointing")
    if (checkpointingEnabled) env.enableCheckpointing(10000)

    val settings = EnvironmentSettings.newInstance.build
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.executeSql(
      """
        | CREATE TABLE collect_evt (
        |   evt STRING,
        |   app STRING,
        |   uid STRING,
        |   ts STRING,
        |   url STRING,
        |   action STRING
        | ) WITH (
        |   'connector' = 'kafka',
        |   'topic' = '%1$s',
        |   'properties.bootstrap.servers' = '%2$s',
        |   'properties.group.id' = '%3$s',
        |   'format' = 'json'
        | )
        |""".stripMargin format(topic, bootStrapServers, groupId))

    tEnv.executeSql(
      """
        | CREATE TABLE collect_evt_es (
        |   evt STRING,
        |   app STRING,
        |   uid STRING,
        |   ts BIGINT,
        |   url STRING,
        |   action STRING,
        |   log_ts TIMESTAMP
        | ) WITH (
        |   'connector' = 'elasticsearch-7',
        |   'hosts' = '%1$s',
        |   'username' = '%3$s',
        |   'password' = '%4$s',
        |   'index' = '%2$s-{log_ts|yyyy-MM-dd}'
        | )
        |""".stripMargin format(esHost, esIndex, esUsername, esPassword)
    )

    tEnv.executeSql(
      """
        | INSERT INTO collect_evt_es
        |   SELECT
        |     evt,
        |     app,
        |     uid,
        |     CAST (ts AS BIGINT) ts,
        |     url,
        |     action,
        |     TO_TIMESTAMP(FROM_UNIXTIME(CAST (ts AS BIGINT) / 1000, 'yyyy-MM-dd HH:mm:ss')) log_ts
        |     FROM collect_evt
        |""".stripMargin).await()

  }
}
