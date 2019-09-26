package com.tqz.scala.course04

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object CounterApp {
  def main(args: Array[String]): Unit = {
    //    errorDemo()
    rightDemo()
  }

  def errorDemo(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("hadoop", "spark", "flink", "strom")
    data.map(new RichMapFunction[String, Long]() {
      var counter: Long = 0L

      override def map(value: String): Long = {
        counter += 1
        println("counter: " + counter)
        counter // 这个玩意儿写在最后表示是返回值
      }
    }).setParallelism(3).print()
    // 并行度为 1 的时候是正常的，但是当并行度大于 1 的时候就异常 了
  }

  def rightDemo(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("hadoop", "spark", "flink", "strom", "sppakk", "sopooa", "sjaygjas")
    data.map(new RichMapFunction[String, String] {
      // 1. 定义计数器
      var counter = new LongCounter()
      override def open(parameters: Configuration): Unit = {
        // 2. 注册计数器
        getRuntimeContext().addAccumulator("counter", counter)
      }
      override def map(in: String): String = {
        // 3. 使用计数器
        counter.add(1)
        in
      }
    }).setParallelism(1)
      .writeAsText("D:\\MyConfiguration\\qinzheng.tian\\IdeaProjects\\flink\\output\\count.txt", WriteMode.OVERWRITE)
    // 4. 取出计数器数据（数据必须 sink 出去）
    val jobResult = env.execute("CounterApp")
    val countNum = jobResult.getAccumulatorResult[Long]("counter")
    // 打印
    println("countNum: " + countNum)
  }
}
