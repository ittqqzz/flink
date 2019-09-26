package com.tqz.scala.project

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

/**
  * Author: Michael PK
  */
class PKMySQLSource extends RichParallelSourceFunction[mutable.HashMap[String,String]]{

  var connection: Connection = null
  var ps:PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://10.101.50.10:3306/flink?useSSL=false"
    val username = "root"
    val p4word = "root"
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, p4word)
    val sql = "select user_id, domain from user_domain_config;"
    ps = connection.prepareStatement(sql)
  }


  override def close(): Unit = {
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  override def cancel(): Unit = {

  }

  // 把从数据库里面读取到的数据转成map
  override def run(sourceContext: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = {

  }
}
