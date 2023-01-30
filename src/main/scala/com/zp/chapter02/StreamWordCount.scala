package com.zp.chapter02

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取文本文件数据
    val linDataStram = env.socketTextStream("master",7777)
    //对数据集进行转换
    val wordAndOne = linDataStram.flatMap(_.split(" ")).map((_, 1))
    //对数据按照单词进行分组
    val wordAndOneGroup = wordAndOne.keyBy(_._1)
    //对数据进行聚合
    val sum = wordAndOneGroup.sum(1)
    //输出数据
    sum.print()
    //
    env.execute()
  }
}
