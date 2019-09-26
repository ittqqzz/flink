package com.tqz.scala.course03

import org.apache.flink.api.common.functions.{FlatMapFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * scala 流式处理
  */
object StreamingWCScalaApp2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("120.79.241.167", 80)

    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)
      .map(new MyMapFunction()) // 使用自定义的 map
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("StreamingWCScalaApp")
  }

  // Define keys using Field Expressions
  // 先定义类，然后使用类的属性作为来分组用的 key
  case class WC(word: String, count: Int)

  // 自定义 map 方法
  class MyMapFunction extends RichMapFunction[String, WC] {
    def map(in: String): WC = {
      new WC(in, 1)
    }
  };


}
