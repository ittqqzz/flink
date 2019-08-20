package com.tqz.scala.course05

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * SourceFunction 的泛型就是该自定义数据源获取到的数据元素的类型，这里假设产生的数据都是整数，所以写Long
  */
object CustomNonParallelSourcesFunction extends SourceFunction[Long] {

  var count = 0L

  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning && count < 1000) {
      // 将从数据源获取到的元素发射出去
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
