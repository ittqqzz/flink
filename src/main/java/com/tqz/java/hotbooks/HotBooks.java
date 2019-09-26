package com.tqz.java.hotbooks;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Auther: qinzheng.tian
 * @Date: 2019/9/26 13:26
 * @Description: 每隔 5 秒钟输出最近 1 小时内点击量最多的前 N 个图书
 */
public class HotBooks {

    public static void main(String[] args) throws Exception{
        // 1. 创建环境，设置 Time
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
          // 1.1 todo 目前暂且将时间设为这个
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // 2. 设置数据源为 kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "120.79.241.167:9092");
        properties.setProperty("enable.auto.commit", "true");
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>("topn", new SimpleStringSchema(), properties);

        DataStream<String> stream = env.addSource(input);

        // 3. 将获取的每一条数据组装成：（句子，1）的格式
        DataStream<Tuple2<String, Integer>> formatStream = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(in, 1));
            }
        });
        // 4. 处理数据，Transformation
        // todo 窗口理解不透彻。。
        DataStream res = formatStream
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2))) // 每隔 2 秒统计最近 5 秒的数据
                .sum(1)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(2)))// 每隔 5 秒后窗口往后翻滚一次
                .process(new TopNAllFunction(3));// 将前面窗口统计的数据拿来排序

        res.print();
        // 5. sink 数据
        res.writeAsText("D:\\MyConfiguration\\qinzheng.tian\\IdeaProjects\\flink\\flink-train\\output\\book"+ System.currentTimeMillis() + ".txt", FileSystem.WriteMode.NO_OVERWRITE);
        env.execute("HotBooks");
    }

}


