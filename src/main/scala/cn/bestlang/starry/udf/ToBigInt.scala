package cn.bestlang.starry.udf

import org.apache.flink.table.functions.ScalarFunction

import java.sql.Timestamp

class ToBigInt extends ScalarFunction {

  def eval(ts: Timestamp) = ts.getTime
}
