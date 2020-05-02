package com.duokao.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * 流处理
 */
object SocketWCDemo {

  case class WordWithCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 获取Socket流
    val text = env.socketTextStream(host, port, '\n')
    val windowCounts = text.flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }

      .keyBy("word")
      .sum("count")

    windowCounts.print()
    // 开始执行任务
    env.execute("print word count job")
  }
}
