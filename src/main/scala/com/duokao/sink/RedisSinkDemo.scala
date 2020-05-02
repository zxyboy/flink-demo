package com.duokao.sink

import java.util.Properties

import com.duokao.transform.Sensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "consumer-test")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("auto.offset.reset", "latest")
    val inputTopic = "sensor"


    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String](inputTopic, new SimpleStringSchema(), properties))
      .map(line => {
        val strings = line.split(",")
        Sensor(strings(0).strip(), strings(1).strip().toDouble, strings(2).strip().toLong)
      })

    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setDatabase(0)
      .setPassword("rootqaz")
      .setPort(6379)
      .build()
    // Sink
    kafkaStream.addSink(new RedisSink(config, new MyRedisMapper))
    kafkaStream.print("print->")


    env.execute("redis sink job")
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