package com.duokao.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object MySQLSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    env.setParallelism(1)

    val filePath = "/Users/xy/IdeaProjects/flink-demo/src/main/resources/sensor.txt"
    val textStream = env.readTextFile(filePath)
      .map(line => {
        val strings = line.split(",")
        Sensor(strings(0).strip(), strings(1).strip().toDouble, strings(2).strip().toLong)
      })


    textStream.assignTimestampsAndWatermarks(
      // Time.milliseconds(1000) 指定watermark延迟一秒
      new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.milliseconds(1000)) {
        // 指定Sensor.timestamp * 1000 为 Event Time
        override def extractTimestamp(element: Sensor): Long = {
          element.timestamp * 1000
        }
      })

    // Sink
    textStream.addSink(new MySQLSink())
    textStream.print("print->")

    env.execute("mysql sink job test")
  }
}

class MySQLSink() extends RichSinkFunction[Sensor] {

  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    println("MySQL open ...............")
    val url = "jdbc:mysql://localhost:3306/test"
    val user = "root"
    val password = "123456"
    conn = DriverManager.getConnection(url, user, password)
    insertStmt = conn.prepareStatement("INSERT INTO temp (sensor , temp) values (? ,? )")
    updateStmt = conn.prepareCall("UPDATE temp SET temp = ? where  sensor = ? ")
  }

  override def invoke(value: Sensor, context: SinkFunction.Context[_]): Unit = {
    //    super.invoke(value, context)
    println("invoke ............")
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }

  }

  override def close(): Unit = {
    println("close ..............")
    super.close()
    insertStmt.close()
    updateStmt.close()
    conn.close()

  }
}


case class Sensor(id: String, temperature: Double, timestamp: Long)
