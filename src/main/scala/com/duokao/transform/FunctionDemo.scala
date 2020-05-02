package com.duokao.transform

import org.apache.flink.api.common.functions.{IterationRuntimeContext, MapFunction, RichFlatMapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

object FunctionDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val textStream = env.readTextFile("/Users/xy/IdeaProjects/flink-demo/src/main/resources/sensor.txt")

    textStream.map(line => {
      val strings = line.split(",")
      Sensor(strings(0).strip(), strings(1).strip().toDouble, strings(2).strip().toLong)
    })

    textStream.map(new MyMapFunction())

    textStream.map(new MapFunction[String, Sensor] {
      override def map(value: String): Sensor = {
        val strings = value.split(",")
        Sensor(strings(0).strip(), strings(1).strip().toDouble, strings(2).strip().toLong)
      }
    })

    //    textStream.map(new MyRichMapFunction).print("print=>")

    textStream.flatMap(new MyRichFlatMapFunction).print("print=>")
    env.execute("transform job")
  }

  class MyRichFlatMapFunction extends RichFlatMapFunction[String, String] {
    override def flatMap(value: String, out: Collector[String]): Unit = {
      value.split(",").foreach(e => out.collect(e))

    }

    override def open(parameters: Configuration): Unit = {
      println(Thread.currentThread().getId + " => MyRichFlatMapFunction open method is invoking now")
    }

    override def close(): Unit = {
      println(Thread.currentThread().getId + " => MyRichFlatMapFunction close method is invoking now")
    }
  }

}


class MyMapFunction() extends MapFunction[String, Sensor] {
  override def map(value: String): Sensor = {
    val strings = value.split(",")
    Sensor(strings(0).strip(), strings(1).strip().toDouble, strings(2).strip().toLong)
  }
}

class MyRichMapFunction() extends RichMapFunction[String, Sensor] {
  override def map(value: String): Sensor = {
    val strings = value.split(",")
    Sensor(strings(0).strip(), strings(1).strip().toDouble, strings(2).strip().toLong)
  }

  override def open(parameters: Configuration): Unit = {
    println(Thread.currentThread().getId + " => Map Function open method is invoking now")
  }

  override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

  override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

  override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

  override def close(): Unit = {
    println(Thread.currentThread().getId + " => Map Function close method is invoking now")
  }
}