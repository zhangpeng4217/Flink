package com.zp.chapter08

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object ConnectTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //定义两条整数流
    val stream1 = env.fromElements(1, 2, 3)
    val stream2 = env.fromElements(4L, 5L, 6L)

    //连接两条流
    stream1.connect(stream2).map(new CoMapFunction[Int,Long,String] {
      override def map1(in1: Int): String = s"Int:${in1}"

      override def map2(in2: Long): String = s"Long:${in2}"
    })
      .print()

    env.execute()
  }
}
