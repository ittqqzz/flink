package com.tqz.scala.course04

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * 把一个dataset 数据集广播出去，然后不同的任务在节点上都能够获取到，这个数据在每个节点上只会存在一份
  */
object BroadcastApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 1. 创建需要广播的数据集
    val data = env.fromElements("a", "b")
    data.map(new RichMapFunction[String, String] {
      var broadcastSet: Traversable[String] = null

      override def open(parameters: Configuration): Unit = {
        // 3. 获取广播出去的数据集
        import scala.collection.JavaConverters._
        val list = broadcastSet = getRuntimeContext().getBroadcastVariable[String]("broadcastSetName").asScala
        for( x <- list:Unit ){

        }
      }

      override def map(value: String): String = {
        value
      }
    }) // 2. 将该数据集广播出去
      .withBroadcastSet(data, "broadcastSetName")
      .print()

  }
}
