package cn.bestlang.starry.evt

import cn.bestlang.starry.udf.ToBigInt
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.junit.jupiter.api.Test

class CollectEvt1mPvJobTest {

  @Test
  def testMain(): Unit = {
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner().inStreamingMode().build
    val tEnv = TableEnvironment.create(settings)

    val collect_evt = getClass.getClassLoader.getResource("collect-evt.log").getFile


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
        |   'connector' = 'filesystem',
        |   'path' = '%1$s',
        |   'format' = 'json'
        | )
        |""".stripMargin format collect_evt)

    tEnv.sqlQuery(
      """
        | SELECT * FROM collect_evt
        |""".stripMargin).execute().print()

    tEnv.sqlQuery(
      """
        |
        |   SELECT
        |     TUMBLE_START(time_ltz, INTERVAL '1' MINUTE) AS wStart,
        |     app,
        |     url,
        |     COUNT(*) AS `count`
        |   FROM collect_evt
        |     WHERE evt = 'pv'
        |     GROUP BY TUMBLE(time_ltz, INTERVAL '1' MINUTE), app, url
        |
        |""".stripMargin).execute().print()


    testEsSink(tEnv)
  }

  def testEsSink(tEnv: TableEnvironment): Unit = {
    val esHost = "localhost:9200"
    val esIndex = "evt-1m-pv"
    val esUsername = "elastic"
    val esPassword = "123456"

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
