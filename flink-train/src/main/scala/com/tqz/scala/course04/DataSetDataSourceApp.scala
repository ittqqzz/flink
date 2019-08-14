package com.tqz.scala.course04

import org.apache.flink.api.scala.ExecutionEnvironment

object DataSetDataSourceApp {
  val PATH: String = "D:\\MyConfiguration\\qinzheng.tian\\IdeaProjects\\flink\\input\\"
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    fromCompressed(env)
  }

  def fromCompressed(env: ExecutionEnvironment): Unit = {
    env.readTextFile(PATH + "\\hello.txt.gz").print()
  }
}
