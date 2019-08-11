package com.tqz.flink.java.course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 词频统计
 */
public class BatchWCJavaApp {

    public static void main(String[] args) throws Exception {
        // 1. 获取开发环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据
        String input = "C:\\Users\\tqz\\IdeaProjects\\flink\\input";
        DataSource<String> text = env.readTextFile(input + "\\hello.txt");
        text.print();

        // 3. 数据操作
        // 3.1 每一行都按照指定的分隔符拆分，然后统计每一个单词出现的次数
        // 3.2 合并操作：group
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {// 进来一个字符串，转成字符串&频率
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 先将每一行的字符读取进来按照制表符拆成数组
                String[] tokens = s.toLowerCase().split("\t");
                for (String token: tokens) {
                    if (token.length() > 0) {
                        // 将数组里面的每一个字符串组成一个新的集合，频率输出哈·初始化为1
                        collector.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        // group 操作：groupBy 的这个 0 就是0号位置的token，然后求和，通过单词出现的次数求和
            // 即，前面是 key，后面是 value
        }).groupBy(0).sum(1).print();


    }
}
