package com.zp.chapter05

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

object RescaleExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    使用匿名类的方式自定义数据源，这里使用了并行数据源函数的复函数版本
    env.addSource(new RichParallelSourceFunction[Int] {
      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        for (i <- 0 to 7) {
          //将偶数发送到下游索引为0的并行子任务中去
          //将奇数发送到下游索引为1的并行子任务中去
          if ((i+1)%2 == getRuntimeContext.getIndexOfThisSubtask){
            sourceContext.collect(i+1)
          }
        }
      }
      //这里的“？？？”是Scala中的占位符
      override def cancel(): Unit = ???
    })
      .setParallelism(2)
      .rescale
      .print()
      .setParallelism(4)

    env.execute()
  }

}
