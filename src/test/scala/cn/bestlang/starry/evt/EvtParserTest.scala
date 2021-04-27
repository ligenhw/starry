package cn.bestlang.starry.evt

import org.junit.jupiter.api.Test

class EvtParserTest {

  val parser = new EvtParser

  @Test
  def testEvalPv(): Unit = {
    val inputStr = "POST /collect?app=invitation-mp&uid=oMKxD5cR3e3a6LZuZWt2lNhI3rXE&ts=1618644022205&url=pages%2Fmap%2Fmap&evt=pv"
    val result = parser.eval(inputStr)
    println(result)
  }

  @Test
  def testEvalCli(): Unit = {
    val inputStr = "POST /collect?app=invitation-mp&uid=oMKxD5cR3e3a6LZuZWt2lNhI3rXE&ts=1618644061697&action=callBride&evt=cli"
    val result = parser.eval(inputStr)
    println(result)
  }

  @Test
  def testParam(): Unit = {
    val sql =
      """
        | 'topic' = '%1$s'
        |""".stripMargin format "nginx-access-log"

    println(sql)
  }

}
