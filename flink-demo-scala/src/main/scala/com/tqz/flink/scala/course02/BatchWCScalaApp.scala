package com.tqz.flink.scala.course02

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * 使用 scala 处理词频统计
  */
object BatchWCScalaApp {
  def main(args: Array[String]): Unit = {
    // 1. 获取环境
    val env = ExecutionEnvironment.getExecutionEnvironment;

    // 2. 读取文件
    val input = "C:\\Users\\tqz\\IdeaProjects\\flink\\input";
    val text = env.readTextFile(input + "\\hello.txt");
    text.print()

    // 引入隐式转换（面试必考）
    import org.apache.flink.api.scala._

    // 3. 转换数据
    text.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
