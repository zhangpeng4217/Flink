package com.zp.chapter05
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment._
object RichFunction {
  def main(args: Array[String]): Unit = {
    //创建连接环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    //获取数据源
    env.fromElements(
      Event("Mary", "./home", 1000L),
        Event("Bob", "./cart", 2000L),
        Event("Alice", "./prod?id=1", 5 * 1000L),
        Event("Cary", "./home", 60 * 1000L)
    )
      .map(new RichMapFunction[Event,Long] {
//      在任务声明周期开始时会执行open方法，在控制台打印对应语句
        override def open(parameters: Configuration): Unit = {
          println("索引为"+getRuntimeContext.getIndexOfThisSubtask+"的任务开始")
        }
//      在任务声明周期结束时会执行close方法，在控制台打印对应语句
        override def close(): Unit = {
          println("索引为"+getRuntimeContext.getIndexOfThisSubtask+"的任务结束")
        }

        override def map(in: Event): Long = in.timestamp
      }).print()
//    让程序执行
    env.execute()

  }
}
