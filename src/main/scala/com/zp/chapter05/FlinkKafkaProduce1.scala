package com.zp.chapter05

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig

object FlinkKafkaProduce1 {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //读取自定义数据源
    val stream = env.addSource(new ClickSource)
    val streamString = stream.map(event => event.toString)
    //创建一个Kafka生产者
    val properties = new Properties()
    properties.put("bootstrap.servers","192.168.137.10:9092")

    val kafkaProduce: FlinkKafkaProducer[String] = new FlinkKafkaProducer("clicks",new SimpleStringSchema(), properties)

    //将生产者和Flink流关联起来
    streamString.addSink(kafkaProduce)

    //触发程序执行
    env.execute()
  }

}
