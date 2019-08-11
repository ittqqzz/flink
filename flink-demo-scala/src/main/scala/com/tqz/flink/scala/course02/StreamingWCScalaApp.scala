package com.tqz.flink.scala.course02

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * scala 流式处理
  */
object StreamingWCScalaApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("120.79.241.167", 80)

    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("StreamingWCScalaApp")
  }

}
