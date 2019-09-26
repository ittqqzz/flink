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
    //socketFunction(env)
//    customNonParallelSource(env)
//    customParallelSource(env)
    customRichParallelSource(env)
    env.execute("Window Stream WordCount")
  }

  def customRichParallelSource(env: StreamExecutionEnvironment): Unit = {
    // 可以并行执行
    val text = env.addSource(CustomRichParallelSourceFunction).setParallelism(2)
    text.print()
  }

  def customParallelSource(env: StreamExecutionEnvironment): Unit = {
    // 可以并行执行
    val text = env.addSource(CustomParallelSourceFunction).setParallelism(2)
    text.print().setParallelism(1)
  }

  def customNonParallelSource(env: StreamExecutionEnvironment): Unit = {
    // 不可以并行执行
    val text = env.addSource(CustomNonParallelSourcesFunction).setParallelism(1) // 只能是 1
    text.print()
  }

  def socketFunction(env: StreamExecutionEnvironment): Unit = {
    val text = env.socketTextStream("54bruce.com", 8580)

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
  }

}
