package com.duokao.source

import java.util.Random

import com.duokao.transform.Sensor
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}


object CustomSourceTest {
  def main(args: Array[String]): Unit = {
    // 执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 自定数据源
    val customStream = env.addSource(new CustomSource())
    customStream.print("custom source -> ")
    env.execute(" source test job")
  }
}

class CustomSource() extends SourceFunction[Sensor] {
  // 标志是否可以继续生成数据
  var flag = true;

  /**
   * 生成数据
   *
   * @param ctx
   */
  override def run(ctx: SourceFunction.SourceContext[Sensor]): Unit = {
    val rand = new Random()

    val tuples = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian())
    )

    val timestamp = System.currentTimeMillis()
    while (flag) {
      tuples.foreach(
        t => {
          val sensor = Sensor(t._1, t._2, timestamp)
          // 将生成的数据发送出去
          ctx.collect(sensor)
        }
      )
    }
    Thread.sleep(500)
  }

  /**
   * 取消生成数据
   */
  override def cancel(): Unit = {
    flag = false
  }
}
