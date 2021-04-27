package cn.bestlang.starry.evt

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object EvtEtlJob {

  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance.build
    val tEnv = TableEnvironment.create(settings)
    val params = ParameterTool.fromArgs(args)

    val topic = params.get("kafka.topic")
    val bootStrapServers = params.get("kafka.bootstrap.servers")
    val groupId = params.get("kafka.group.id")
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
        |   'connector' = 'kafka',
        |   'topic' = '%1$s',
        |   'properties.bootstrap.servers' = '%2$s',
        |   'properties.group.id' = '%3$s',
        |   'format' = 'json'
        | )
        |""".stripMargin format(topic, bootStrapServers, groupId))


    val esHost = params.get("es.host")
    val esIndex = params.get("es.index")
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
        |   'connector' = 'elasticsearch-7',
        |   'hosts' = '%1$s',
        |   'index' = '%2$s'
        | )
        |""".stripMargin format(esHost, esIndex))

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
