package com.zp.chapter05

import org.apache.flink.streaming.api.scala._

object RebalanceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
//    经轮训重分区后打印输出，并行度为4
    stream.rebalance.print("rebalance").setParallelism(4)

    env.execute()
  }

}
