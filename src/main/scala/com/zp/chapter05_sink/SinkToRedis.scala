package com.zp.chapter05_sink
import com.zp.chapter06.ClickSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object SinkToRedis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //添加相应数据源
    val stream = env.addSource(new ClickSource)
    //相应的连接配置   JFlinkJedisConfigBase：Jedis 的连接配置。
    val conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.137.30").setPassword("123456").build()
    //添加相应Sink
    stream.addSink(new RedisSink[Event](conf,new MyRedisMapper))

    env.execute()
  }
  //RedisMapper：Redis 映射类接口，说明怎样将数据转换成可以写入 Redis 的类型。
  //接下来主要就是定义一个 Redis 的映射类，实现 RedisMapper 接口。
  class MyRedisMapper extends RedisMapper[Event]{
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"clicks")

    override def getKeyFromData(t: Event): String = t.user

    override def getValueFromData(t: Event): String = t.url
  }
}
