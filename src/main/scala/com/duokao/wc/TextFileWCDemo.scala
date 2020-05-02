package com.duokao.wc

import org.apache.flink.api.scala._

// 必须引入隐私转换

/**
 * 批处理
 */
object TextFileWCDemo {

  case class WordWithCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {
    val filePath = "/Users/xy/IdeaProjects/flink-demo-1/src/main/resources/wc.txt"
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val text = environment.readTextFile(filePath)

    val count = text.flatMap(line => line.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .groupBy("word")
      .sum("count")
    count.print()
  }
}
