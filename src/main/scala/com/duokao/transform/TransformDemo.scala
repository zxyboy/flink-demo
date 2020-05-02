package com.duokao.transform

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation

object TransformDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val splitStream = env.readTextFile("/Users/xy/IdeaProjects/flink-demo/src/main/resources/sensor.txt")
      .map(line => {
        val strings = line.split(",")
        Sensor(strings(0).strip(), strings(1).strip().toDouble, strings(2).strip().toLong)
      }).split(sensor => {
      // 按照温度划分流，>60 分成high流，否则分为low流
      if (sensor.temperature > 60) Seq("high") else Seq("low")
    })

    val highStream = splitStream.select("high")
    val lowStream = splitStream.select("low")
    val highOrLowStream = splitStream.select("high", "low")

    val unionStream = highStream.union(lowStream).union(highOrLowStream)


    val warmingStream = highStream.map(sensor => (sensor.id, sensor.temperature, "warming"))
    //     warmingStream和lowStream的泛型可以不一样
    val stream = warmingStream.connect(lowStream)
      .map(warming => warming, sensor => (sensor.id, sensor.temperature, "healthy"))

    val stream2 = warmingStream.connect(lowStream)
      // map中两个函数返回值泛型可以不同
      .map(warming => warming, sensor => sensor)

    stream.print("stream->")
    stream2.print("stream2->")

    //    highStream.print("highStream ->")
    //    lowStream.print("lowStream ->")
    //    highOrLowStream.print("highOrLowStream ->")

    //      .keyBy("id")
    //      .sum("temperature")
    // 输入当前传感器温度 + 10 ， 时间戳是上一次时间戳
    //      .reduce((x, y) => Sensor(x.id, x.temperature + 10, y.timestamp))


    env.execute("transform job")
  }
}

case class Sensor(id: String, temperature: Double, timestamp: Long)