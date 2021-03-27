package cn.bestlang.starry.job

import org.junit.jupiter.api.Test

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

class AccessLogParserTest {

  val parser = new AccessLogParser

  @Test private[job] def eval(): Unit = {
    val inputStr = "39.97.119.210 - - [25/Mar/2021:14:47:36 +0800] \"POST /collect?app=invitation-mp&uid=test-user&ts=1616139198232&url=post&evt=pv HTTP/1.0\" 200 0 \"-\" \"PostmanRuntime/7.26.10\" \"111.202.148.42\"";
    val result = parser.eval(inputStr)
    println(result)
  }

  @Test
  def wx() = {
    val inputStr = "39.97.119.210 - - [27/Mar/2021:11:26:05 +0800] \"POST /collect HTTP/1.0\" 200 0 \"https://servicewechat.com/wxdbb8f0a9cc0c0a9c/devtools/page-frame.html\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.3 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1 wechatdevtools/1.05.2103262 MicroMessenger/7.0.4 Language/zh_CN webview/\" \"124.200.128.109\""
    val result = parser.eval(inputStr)
    println(result)
  }

  @Test
  def parseTime(): Unit = {
    val text = "25/Mar/2021:14:47:36 +0800"
    val format = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
    val time = LocalDateTime.parse(text, format)
    println(time)
    println(time.toInstant(ZoneOffset.of("+08")).toEpochMilli)
    println(System.currentTimeMillis())
  }

  @Test
  def parseHttpUserAgent(): Unit = {
    val text = "\"Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.3 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1 wechatdevtools/1.05.2103262 MicroMessenger/7.0.4 Language/zh_CN webview/\" \"111.202.148.42\""
    val p =     "\\\"([^\\\"]*)\\\" \\\"([^\\\"]*)\\\""
    val pattern = Pattern.compile(p)
    val m = pattern.matcher(text)
    m.find()

    println(m.group(1))
    println(m.group(2))
  }
}
