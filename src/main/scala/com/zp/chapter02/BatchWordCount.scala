package com.zp.chapter02

import org.apache.flink.api.scala._
object BatchWordCount {
  def main(args: Array[String]): Unit = {
//    TODO 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
//    TODO 读取文本数据
    val lineDataSet = env.readTextFile("input/words.txt")
//    TODO 对数据进行转换处理
    val wordAndOne = lineDataSet.flatMap(_.split(" ")).map(word => (word, 1))
//    TODO 按照单词进行分组
    val wordAndOneGroup = wordAndOne.groupBy(0)
//    TODO 对分组数据进行sum聚合统计
    val sum = wordAndOneGroup.sum(1)
//    TODO 打印输出
    sum.print()
  }

}
