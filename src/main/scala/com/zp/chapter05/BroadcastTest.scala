package com.zp.chapter05

import org.apache.flink.streaming.api.scala._

object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //读取数据，并行度为1
    val stream = env.addSource(new ClickSource)
    stream.broadcast.print("broadcast").setParallelism(4)
    env.execute()
  }
}
