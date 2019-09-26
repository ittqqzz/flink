package com.tqz.scala.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object DataSetTransformationApp {

  val env = ExecutionEnvironment.getExecutionEnvironment
  val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

  def main(args: Array[String]): Unit = {
    //    mapFunction()
    //    filerFunction()
    //    mapPartitionFunction()
    //    firstFunction()
    //    flatMapFunction()
    //    distinctFunction()
    //    joinFunction()
    //    outerJoinFunction()
    crossFunction()
  }

  def crossFunction(): Unit = {
    val info1 = List("曼联", "费城")
    val info2 = List(3, 1, 0)

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.cross(data2).print()

  }

  def outerJoinFunction(): Unit = {
    val info1 = ListBuffer[(Int, String)]() // 编号  名字
    info1.append((1, "PK哥"));
    info1.append((2, "J哥"));
    info1.append((3, "小队长"));
    info1.append((4, "猪头呼"));

    val info2 = ListBuffer[(Int, String)]() // 编号  城市
    info2.append((1, "北京"));
    info2.append((2, "上海"));
    info2.append((3, "成都"));
    info2.append((5, "杭州"));

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    // 左连接的特点就是左边的全部都有。但是右边的只要匹配上的，下面的同理
    //    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
    //      if (second == null) {
    //        (first._1, first._2, "-")
    //      } else {
    //        (first._1, first._2, second._2)
    //      }
    //    }).print()

    //    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
    //      if (first == null) {
    //        (second._1, "-", second._2)
    //      } else {
    //        (first._1, first._2, second._2)
    //      }
    //    }).print()

    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {

      if (first == null) {
        (second._1, "-", second._2)
      } else if (second == null) {
        (first._1, first._2, "-")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()
  }

  def joinFunction(): Unit = {
    val info1 = ListBuffer[(Int, String)]() // 编号  名字
    info1.append((1, "PK哥"));
    info1.append((2, "J哥"));
    info1.append((3, "小队长"));
    info1.append((4, "猪头呼"));

    val info2 = ListBuffer[(Int, String)]() // 编号  城市
    info2.append((1, "北京"));
    info2.append((2, "上海"));
    info2.append((3, "成都"));
    info2.append((5, "杭州"));

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0).apply((first, second) => {
      // 最终的结果取一个 Tuple，Tuple 的写法就是一个括号
      (first._1, first._2, second._2)
    }).print()

  }

  def distinctFunction(): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    val data = env.fromCollection(info)

    data.flatMap(_.split(",")).distinct().print()
  }

  def flatMapFunction(): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")

    val data = env.fromCollection(info)
    //    data.print()
    //data.map(_.split(",")).print()

    // 把一个元素变成多个。也就是将"flink,flink"解析成"flink"  "flink" 两个元素
    //    data.flatMap(_.split(",")).print()

    data.flatMap(_.split(",")).map((_, 1)).groupBy(0).sum(1).print()
  }


  def firstFunction(): Unit = {
    val info = ListBuffer[(Int, String)]()
    info.append((1, "Hadoop"));
    info.append((1, "Spark"));
    info.append((1, "Flink"));
    info.append((2, "Java"));
    info.append((2, "Spring Boot"));
    info.append((3, "Linux"));
    info.append((4, "VUE"));

    val data = env.fromCollection(info)

    //    data.first(3).print()
    // 按照 inr 字段分组，然后只取每组的前两个
    //    data.groupBy(0).first(2).print()

    // 分组之后，在每一组里面按照第一个元素（从 0 开始），降序排序
    data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print()
  }

  // DataSource 100个元素，把结果存储到数据库中
  def mapPartitionFunction(): Unit = {
    val students = new ListBuffer[String]
    for (i <- 1 to 100) {
      students.append("student: " + i)
    }
    /**
      * map是对rdd中的每一个元素进行操作，function要执行和计算很多次
      * mapPartitions则是对rdd中的每个分区的迭代器进行操作，一个task仅仅会执行一次function，function一次接收所有
      * 的partition数据。只要执行一次就可以了，性能比较高
      *
      * 操作数据库的时候：map需要为每个元素创建一个链接而mapPartition为每个partition创建一个链接
      */
    val data = env.fromCollection(students).setParallelism(10)
    //    data.map(x => {
    //      // 每一个元素要存储到数据库中去，肯定需要先获取到一个connection
    //      val connection = DBUtils.getConection()
    //      println(connection + "....")
    //
    //      //TODO... 保存数据到DB
    //      DBUtils.returnConnection(connection)
    //    }).print()


    data.mapPartition(x => {
      val connection = DBUtils.getConection()
      println(connection + "....")
      //TODO... 保存数据到DB
      DBUtils.returnConnection(connection)
      x
    }).print()

  }

  def mapFunction(): Unit = {
    //    data.map(x => x+1).print()
    data.map(_ + 1).print()
  }

  def filerFunction(): Unit = {
    data.filter(_ > 5).print()
  }
}
