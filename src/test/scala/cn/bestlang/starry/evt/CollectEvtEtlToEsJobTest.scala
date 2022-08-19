package cn.bestlang.starry.evt

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.junit.Test

class CollectEvtEtlToEsJobTest {

  def readFromResource(file: String): String = {
    val source = s"${getClass.getResource("/").getFile}../../src/test/resources/$file"
    source
  }

  @Test
  def testMain(): Unit = {
    val settings = EnvironmentSettings.newInstance.inStreamingMode().build
    val tEnv = TableEnvironment.create(settings)

    val filename = "nginx-parsed.log"

    val collect_evt = readFromResource(filename)
    println(s"CollectEvt1mPvJobTest collect_evt : $collect_evt")

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
        |   'connector' = 'filesystem',
        |   'path' = '%1$s',
        |   'format' = 'json'
        | )
        |""".stripMargin format collect_evt)

    tEnv.executeSql(
      """
        | CREATE TABLE collect_evt_print (
        |   evt STRING,
        |   app STRING,
        |   uid STRING,
        |   ts BIGINT,
        |   url STRING,
        |   action STRING,
        |   log_ts TIMESTAMP
        | ) WITH (
        |   'connector' = 'print'
        | )
        |"""stripMargin
    )

    tEnv.executeSql(
      """
        |   SELECT
        |     evt,
        |     app,
        |     uid,
        |     CAST (ts AS BIGINT) ts,
        |     url,
        |     action,
        |     TO_TIMESTAMP(FROM_UNIXTIME(CAST (ts AS BIGINT) / 1000, 'yyyy-MM-dd HH:mm:ss')) log_ts
        |     FROM (
        |         SELECT evt, app, uid, ts, url, action FROM collect_evt WHERE ts <> ''
        |     )
        |""".stripMargin).print()

  }

}
