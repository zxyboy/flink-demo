package com.duokao.sink

import com.duokao.transform.Sensor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val filePath = "/Users/xy/IdeaProjects/flink-demo/src/main/resources/sensor.txt"
    val kafkaStream = env.readTextFile(filePath)
      .map(line => {
        val strings = line.split(",")
        Sensor(strings(0).strip(), strings(1).strip().toDouble, strings(2).strip().toLong)
      })

    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setDatabase(0)
      .setPassword("rootqaz123")
      .setPort(6379)
      .build()
    // Sink
    kafkaStream.addSink(new RedisSink(config, new MyRedisMapper))
    kafkaStream.print("print->")
    
    env.execute("redis sink job test")
  }
}

/**
 * 将数据保存到redis hashtable数据结构
 */
class MyRedisMapper() extends RedisMapper[Sensor] {
  /**
   * 保存Redis保存命令
   * hset key  field value
   *
   * @return
   */
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor")
  }

  /**
   * 获取 field
   *
   * @param t
   * @return
   */
  override def getKeyFromData(t: Sensor): String = t.id

  /**
   * 获取 value
   *
   * @param t
   * @return
   */
  override def getValueFromData(t: Sensor): String = t.temperature.toString

}