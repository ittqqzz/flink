package com.tqz.flink.java.course03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 实时流式处理
 */
public class StreamingWCJavaApp {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 获取数据
        DataStreamSource<String> text = env.socketTextStream("120.79.241.167", 80);

        // 3. 操作
        // new FlatMapFunction 中第一个泛型代表入参，第二个代表输出数据的类型
        text.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String s, Collector<WC> collector) throws Exception {
                String[] tokens = s.toLowerCase().split(",");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new WC(token, 1));
                    }
                }
            }
            // 使用嵌套的元组的时候如何使用 key ，就是直接传参数的名称
        })//.keyBy("word")
                .keyBy(new KeySelector<WC, String>() {
                    @Override
                    public String getKey(WC value) throws Exception {
                        return value.word;
                    }
                })
                .timeWindow(Time.seconds(5))
                .sum("count")
                .print();

        env.execute("StreamingWCJavaApp");
    }

    public static class WC {
        private String word;
        private Integer count;

        public WC() {
        }


        public WC(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
