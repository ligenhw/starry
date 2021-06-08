package cn.bestlang.starry.job

import cn.bestlang.starry.evt.EvtParser
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object NginxAccessLogCollectEvtEtlToKafkaJob {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    val topic = params.get("kafka.topic")
    val bootStrapServers = params.get("kafka.bootstrap.servers")
    val groupId = params.get("kafka.group.id")
    val sinkTopic = params.get("sink.kafka.topic")
    val sinkBootStrapServers = params.get("sink.kafka.bootstrap.servers")
    val checkpointingEnabled = params.has("checkpointing")
    if (checkpointingEnabled) env.enableCheckpointing(10000)

    val settings = EnvironmentSettings.newInstance.build
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.executeSql(
      """
        | CREATE TABLE nginx_log (
        |        log STRING
        |      ) WITH (
        |        'connector' = 'kafka',
        |        'topic' = '%1$s',
        |        'properties.bootstrap.servers' = '%2$s',
        |        'properties.group.id' = '%3$s',
        |        'format' = 'raw'
        |      )
        |""".stripMargin format(topic, bootStrapServers, groupId)
    )

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
        |   'format' = 'json'
        | )
        |""".stripMargin format(sinkTopic, sinkBootStrapServers))

    // 注册函数
    tEnv.createTemporarySystemFunction("AccessLogParser", classOf[AccessLogParser])
    tEnv.createTemporarySystemFunction("EvtParser", classOf[EvtParser])

    // 运行一个 INSERT 语句，将源表的数据输出到结果表中
    val tableResult = tEnv.executeSql(
      """
        |INSERT INTO collect_evt
        |SELECT t.* FROM (
        |  SELECT EvtParser(t.request) as t FROM
        |    (SELECT AccessLogParser(log) as t FROM nginx_log)
        |    )
        |""".stripMargin)

    // 通过 TableResult 来获取作业状态
    println(tableResult.getJobClient().get().getJobStatus())
  }


}
