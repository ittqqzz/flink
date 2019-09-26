package com.tqz.java.course04;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class CounterApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> list = new ArrayList<>(60);
        for (int i = 1; i < 61; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);
        data.map(new RichMapFunction<Integer, Integer>() {
            // 1. 创建计数器
            IntCounter counter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                // 2. 注册计数器
                getRuntimeContext().addAccumulator("counter", counter);
            }

            @Override
            public Integer map(Integer in) throws Exception {
                // 3. 使用计数器
                counter.add(1);
                return in + 1; // 意思是将输入的每一个值都 map 为 _+1 的样子
            }
        }).setParallelism(1)
                .writeAsText("D:\\MyConfiguration\\qinzheng.tian\\IdeaProjects\\flink\\output\\amount", FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult jobExecutionResult = env.execute();
        Integer countNum = jobExecutionResult.getAccumulatorResult("counter");
        System.out.println(countNum);
    }
}
