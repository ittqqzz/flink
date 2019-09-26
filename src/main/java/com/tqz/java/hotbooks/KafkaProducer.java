package com.tqz.java.hotbooks;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Auther: qinzheng.tian
 * @Date: 2019/9/26 13:26
 * @Description: 用 flink 往 kafka 里面制造数据，kafka 包里面的是直接用 kafka API 制造数据
 */
public class KafkaProducer {

    public static void main(String[] args) throws Exception {
        // 1. 创建环境并添加数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);
        // 2. 配置 kafka 的连接信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "120.79.241.167:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("topn", new SimpleStringSchema(), properties);

        // TODO event-timestamp 事件的发生时间，尝试使用不同的时间处理，看得到的结果的变化
        // producer.setWriteTimestampToKafka(true);

        // 3. 执行 flink 往 kafka 的 sink 任务
        text.addSink(producer);
        env.execute();
    }
}
