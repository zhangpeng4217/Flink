package com.zp.chapter05

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object SourceKafkaTest {
  def main(args: Array[String]): Unit = {
//    创建执行环境
val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1) //设置并行度是为了方便测试
//    用Java配置类Properties保存Kafka连接相关配置
val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.137.10:9092")
//    properties.setProperty("group.id", "consumer-group")
    //    创建一个FlinkKafkaConsumer对象，传入必要参数，从kafka中读取数据   new SimpleStringSchema()序列化作用
val stream = env.addSource(new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), properties))
    stream.print("kafka")
    env.execute()
  }
}
