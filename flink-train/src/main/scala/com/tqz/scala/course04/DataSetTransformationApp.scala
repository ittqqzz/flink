package com.tqz.scala.course04
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

object DataSetTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    mapFunction(env)
  }

  def mapFunction(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(List(1,2,3,4,5,6,7,8,9))
//    data.map(x => x+1).print()
    data.map(_ + 1).print()
  }
}
