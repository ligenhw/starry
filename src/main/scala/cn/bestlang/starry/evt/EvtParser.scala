package cn.bestlang.starry.evt

import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

import java.net.URLDecoder
import java.util.regex.Pattern

class EvtParser extends ScalarFunction {

  /**
   * 输入样例
   *
   * POST /collect?app=invitation-mp&uid=oMKxD5cR3e3a6LZuZWt2lNhI3rXE&ts=1618644022205&url=pages%2Fmap%2Fmap&evt=pv
   */
  private val inputRegex = "\\S+ /\\S+\\?(\\S+)"
  private val pattern = Pattern.compile(inputRegex)

  @DataTypeHint("ROW<evt STRING, app STRING, uid STRING, ts STRING, url STRING, action STRING>")
  def eval(line: String) = {
    val mat = pattern.matcher(line)

    var evt: EvtModel = null
    var paramMap = Map[String, String]()
    if (!mat.find()) {
      evt = EvtModel()
    } else {
      var query = mat.group(1)
      query = URLDecoder.decode(query, "utf-8")

      val paramArray = query.split('&')

      paramArray.foreach(param => {
        val value = if (param.split("=").size > 1) param.split("=")(1) else ""
        paramMap += (param.split("=")(0) -> value)
      })
      evt = EvtParser.createCaseClass[EvtModel](paramMap)
    }

    Row.of(evt.evt, evt.app, evt.uid, evt.ts, evt.url, evt.action)
  }
}

object EvtParser {
  private val log = LoggerFactory.getLogger(EvtParser.getClass)

  def createCaseClass[T](vals: Map[String, String])(implicit cmf: ClassManifest[T]) = {
    val ctor = cmf.erasure.getConstructors.head

    val args = cmf.erasure.getDeclaredFields.map(f => vals.getOrElse(f.getName, null))
    ctor.newInstance(args: _*).asInstanceOf[T]
  }

}