package cn.bestlang.starry.job

import cn.bestlang.starry.job.AccessLogParser._
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.Locale
import java.util.regex.Pattern

class AccessLogParser extends ScalarFunction {

  /**
   * nginx 日志格式
   *
   *     log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
                      '$status $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for"';
   *
   */

  /**
   * 输入样例
   *
   * 39.97.119.210 - - [25/Mar/2021:11:29:06 +0800] "POST /collect?app=invitation-mp&uid=test-user&ts=1616139198232&url=post&evt=pv HTTP/1.0" 200 0 "-" "PostmanRuntime/7.26.10" "111.202.148.42"
   */
  // 正则表达式 https://developer.mozilla.org/zh-CN/docs/Web/JavaScript/Guide/Regular_Expressions
  private val inputRegex = "(\\S+) - (\\S+) \\[(\\S+\\s\\S+)\\] \\\"(\\S+\\s\\S+)\\s\\S+\\\" (\\d+) (\\d+) \\\"(\\S+)\\\" \\\"([^\\\"]*)\\\" \\\"(\\S+)\\\""
  private val pattern = Pattern.compile(inputRegex)

  /**
   * remote_addr (\S+)
   * remote_user (\S+)
   * time_local [(\S+\s\S+)\]
   * http_user_agent "([^"]*)"  匹配引号中内容   [^"] 表示不包含引号 * 表示 匹配前一个表达式 0 次或多次
   *
   * @param line
   * @return
   */
  @DataTypeHint("ROW<remote_addr STRING, remote_user STRING, time_local BIGINT, request STRING, status STRING, body_bytes_sent STRING, http_referer STRING, http_user_agent STRING, http_x_forwarded_for STRING>")
  def eval(line: String) = {
    val mat = pattern.matcher(line)

    if (!mat.find()) {
      // 增加两个字段 存储是否解析成功 和 错误消息的原消息
      log.error("parse failed msg : {}", line)
      Row.of("", "", long2Long(Instant.now().toEpochMilli) ,"", "", "","", "", "")
    } else {
      Row.of(mat.group(1), mat.group(2), parseLocalTimeToTs(mat.group(3)), mat.group(4),
        mat.group(5), mat.group(6), mat.group(7), mat.group(8),
        mat.group(9))
    }
  }

  def parseLocalTimeToTs(local_time: String)   = {
    val time = LocalDateTime.parse(local_time, format)
    val ts = time.toInstant(ZoneOffset.of("+08")).toEpochMilli
    long2Long(ts)
  }
}
object AccessLogParser {
  private val format = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  private val log = LoggerFactory.getLogger(AccessLogParser.getClass)
}