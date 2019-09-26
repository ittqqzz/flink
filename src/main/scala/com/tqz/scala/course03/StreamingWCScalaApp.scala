package com.tqz.scala.course03

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
    /**
      * 注意：scala 中的 tuple 起始元素下标从 1 开始
      */
    text.flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)
      .map(x => WC(x, 1)) // 使用 Field Expressions
      //.keyBy("word")  // 通过类的属性名作为 key 值
      .keyBy(_.word) // 使用 Key Selector Functions
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("StreamingWCScalaApp")
  }

  // Define keys using Field Expressions
  // 先定义类，然后使用类的属性作为来分组用的 key
  case class WC(word: String, count: Int)

}
