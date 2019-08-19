package com.tqz.scala.course05

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 使用 Scala 做数据流Demo
  *
  * 注意批处理的执行环境是 ExecutionEnvironment
  * 但是流处理的执行环境是 StreamExecutionEnvironment，并且流处理最后要执行 execute 方法
  */
object DataStreamDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("54bruce.com", 80)

    val counts = text.flatMap {
      _.toLowerCase.split(",") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print()

    env.execute("Window Stream WordCount")
  }

}
