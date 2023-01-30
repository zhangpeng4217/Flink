package com.zp.chapter05

import org.apache.flink.streaming.api.scala._

object ShuffleTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据源，并行度为1
    val stream = env.addSource(new ClickSource)
    //经洗牌后打印输出，并行度为4
    stream.shuffle.print("Shuffle").setParallelism(4)

    env.execute()
  }

}
