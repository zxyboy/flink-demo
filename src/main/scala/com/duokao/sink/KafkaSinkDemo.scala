package com.duokao.sink

import java.util.Properties

import com.duokao.transform.Sensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkDemo {
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
        val sensor = Sensor(strings(0).strip(), strings(1).strip().toDouble, strings(2).strip().toLong)
        sensor.toString
      })

    // Sink
    val brokerList = "localhost:9092"
    val outputTopic = "sensor-test"
    kafkaStream.addSink(new FlinkKafkaProducer011[String](brokerList, outputTopic, new SimpleStringSchema()))
    kafkaStream.print("print->")


    env.execute("transform job")
  }
}
