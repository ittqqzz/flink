package com.tqz.scala.course04

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.commons.io.FileUtils

/**
  * 详细说明
  * https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/batch/#distributed-cache
  */
object DistributedCacheApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val PATH = "D:\\MyConfiguration\\qinzheng.tian\\IdeaProjects\\flink\\input"
    // 1. 注册一个本地（或者HDFS）文件
    env.registerCachedFile(PATH + "\\hello.txt", "dcCache")

    val data = env.fromElements("hadoop", "spark", "flink", "strom", "sppakk", "sopooa", "sjaygjas")
    data.map(new RichMapFunction[String, String] {

      override def open(parameters: Configuration): Unit = {
        // 2. 在 open 方法里面获取分布式缓存的内容（这个内容是在缓存里面就有的，直接获取）
        val dcFile = getRuntimeContext.getDistributedCache.getFile("dcCache")
        val lines = FileUtils.readLines(dcFile)
        import scala.collection.JavaConverters._
        for (item <- lines.asScala) {
          print(item + "... ")
        }
      }

      override def map(value: String): String = {
        value
      }
    }).print()
  }
}
