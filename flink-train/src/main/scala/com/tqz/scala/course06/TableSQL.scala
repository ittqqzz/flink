package com.tqz.scala.course06

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row

object TableSQL {
  def main(args: Array[String]): Unit = {
    val PATH = "D:\\MyConfiguration\\qinzheng.tian\\IdeaProjects\\flink\\input"
    // 1. 获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2. 获取 Table 运行环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    // 3. 获取数据源
    val csv = env.readCsvFile[Seals](PATH + "\\seals.csv", ignoreFirstLine = true)
    // 4. 操作数据
      // 4.1 将 DataSet 转为 Table
    val sealsTable = tableEnv.fromDataSet(csv)
      // 4.2 将 Table 注册成一张表
    tableEnv.registerTable("sealsTable", sealsTable)
      // 4.3 在表上面执行 sql 语句
    val resTable = tableEnv.sqlQuery("select sum(amountPaid) as profit from sealsTable")
      // 4.4 查看sql执行结果
    tableEnv.toDataSet[Row](resTable).print()
  }

  case class Seals(transcationID:String,
                   customerID:String,
                   itemID:String,
                   amountPaid:Double)
}
