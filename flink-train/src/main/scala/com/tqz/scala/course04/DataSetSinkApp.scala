package com.tqz.scala.course04

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem
/**
  * 设置了并行度，输出的时候，输出路径就是文件夹（不管是否加了后缀）
  * 没有设置并行度（也就是并行度为 1 的时候就是文件）
  */
object DataSetSinkApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = 1 to 10
    val text = env.fromCollection(data)
    text.writeAsText("D:\\MyConfiguration\\qinzheng.tian\\IdeaProjects\\flink\\output\\out1.txt",
      FileSystem.WriteMode.OVERWRITE)
    env.execute("DataSetSinkApp")
  }

}
