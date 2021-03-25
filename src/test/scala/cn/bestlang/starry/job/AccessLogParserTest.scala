package cn.bestlang.starry.job

import org.junit.jupiter.api.Test

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset, ZonedDateTime}
import java.util.Locale

class AccessLogParserTest {

  val parser = new AccessLogParser

  @Test private[job] def eval(): Unit = {
    val inputStr = "39.97.119.210 - - [25/Mar/2021:14:47:36 +0800] \"POST /collect?app=invitation-mp&uid=test-user&ts=1616139198232&url=post&evt=pv HTTP/1.0\" 200 0 \"-\" \"PostmanRuntime/7.26.10\" \"111.202.148.42\"";
    val result = parser.eval(inputStr)
    println(result)
  }

  @Test
  def time(): Unit = {
    val text = "25/Mar/2021:14:47:36 +0800"
    val format = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
    val time = LocalDateTime.parse(text, format)
    println(time)
    println(time.toInstant(ZoneOffset.of("+08")).toEpochMilli)
    println(System.currentTimeMillis())
  }
}
