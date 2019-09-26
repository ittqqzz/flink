package com.tqz.scala.course05
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object DataStreamTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    filterFunction(env)

    env.execute("DataStreamTransformationApp")
  }

  def filterFunction(env: StreamExecutionEnvironment): Unit = {
    val text = env.addSource(CustomNonParallelSourcesFunction)
    text.map(x => {
      x+1
    }).filter(_%2 == 0).print()
  }
}
