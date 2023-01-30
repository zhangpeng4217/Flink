package com.zp.chapter08

import com.zp.chapter05.ClickSource
import org.apache.flink.streaming.api.environment._

object SplitStreamByFilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new ClickSource)
//    filter(条件) 条件成立后数据留下，不成立就过滤掉  利用filter简单的拆分流
    val zpStream = stream.filter(_.user == "zp")
    val czxStream = stream.filter(_.user == "czx")
    val elseStream = stream.filter(data => data.user != "zp" && data.user != "czx")

    zpStream.print("zp")
    czxStream.print("czx")
    elseStream.print("else")

    env.execute()

  }
}
