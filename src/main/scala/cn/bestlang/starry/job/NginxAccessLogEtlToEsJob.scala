package cn.bestlang.starry.job

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object NginxAccessLogEtlToEsJob {

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

    tEnv.executeSql("""
      CREATE TABLE nginx_log (
        log STRING
      ) WITH (
        'connector' = 'kafka',
        'topic' = '%1$s',
        'properties.bootstrap.servers' = '%2$s',
        'properties.group.id' = '%3$s',
        'format' = 'raw'
      )
      """.stripMargin format(topic, bootStrapServers, groupId)
    )

    // 注册函数
    tEnv.createTemporarySystemFunction("AccessLogParser", classOf[AccessLogParser])

    tEnv.executeSql("""
      CREATE TABLE nginx_log_index (
        remote_addr STRING,
        remote_user STRING,
        time_local TIMESTAMP,
        request STRING,
        status STRING,
        body_bytes_sent STRING,
        http_referer STRING,
        http_user_agent STRING,
        http_x_forwarded_for STRING,
        ts BIGINT
      ) WITH (
        'connector' = 'elasticsearch-7',
        'hosts' = '%1$s',
        'username' = '%3$s',
        'password' = '%4$s',
        'index' = '%2$s-{time_local|yyyy-MM-dd}'
      )
    """.stripMargin format(esHost, esIndex, esUsername, esPassword)
    )

    // 运行一个 INSERT 语句，将源表的数据输出到结果表中
    val tableResult = tEnv.executeSql(
      """
        |INSERT INTO nginx_log_index
        |  SELECT
        |     t.remote_addr,
        |     t.remote_user,
        |     TO_TIMESTAMP(FROM_UNIXTIME(t.time_local / 1000, 'yyyy-MM-dd HH:mm:ss')) time_local,
        |     t.request,
        |     t.status,
        |     t.body_bytes_sent,
        |     t.http_referer,
        |     t.http_user_agent,
        |     t.http_x_forwarded_for,
        |     t.time_local ts
        |    FROM
        |    (SELECT AccessLogParser(log) as t FROM nginx_log)
        |""".stripMargin)

    // 通过 TableResult 来获取作业状态
    println(tableResult.getJobClient().get().getJobStatus())
  }


}
