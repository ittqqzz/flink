package com.tqz.java.course05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


import java.util.Arrays;

/**
 * 注意批处理的执行环境是 ExecutionEnvironment
 * 但是流处理的执行环境是 StreamExecutionEnvironment，并且流处理最后要执行 execute 方法
 */
public class DataStreamDataSourceApp {

    public static void main(String[] args) throws Exception{
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 获取数据  & 3. 操作数据
        socketFunction(env);

        // 4. 执行程序
        env.execute("DataStreamDataSourceApp");
    }

    public static void socketFunction(StreamExecutionEnvironment env) {
        // 2. 获取数据
        DataStreamSource<String> text = env.socketTextStream("120.79.241.167", 8580);

        // 3. 操作数据：统计单词频率
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = in.split(",");
                for (String token: tokens) {
                    if (token.length() > 0) {
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        }).keyBy(0)
                //.countWindow(1) // 每个窗口里面只要放入了一个元素就会被触发
                .countWindow(2) // 每个窗口里面只要放入了 2 个元素就会被触发
                .sum(1).print();

    }
}
