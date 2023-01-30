package com.zp.chapter05

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object TransCustomPartitioner {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1,2,3,4,5,6,7)
      .partitionCustom(new Partitioner[Int] {
        //根据奇偶性计算出将发送到那个分区
//        自定义分区器
//        这可能是闭包检测，两个参数
        override def partition(k: Int, i: Int): Int = k % 2
      },data => data)//以自身作为key
      .print()
    env.execute()
  }
}
