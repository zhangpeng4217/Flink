package com.zp.chapter05

import org.apache.flink.streaming.api.scala._

object SourceCustomTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取自定义数据源
    val stream = env.addSource(new ClickSource)
    stream.print("clicksource")
    env.execute()
  }
}
