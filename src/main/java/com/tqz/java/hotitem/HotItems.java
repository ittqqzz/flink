package com.tqz.java.hotitem;

import com.tqz.java.hotitem.count.CountAgg;
import com.tqz.java.hotitem.count.ItemViewCount;
import com.tqz.java.hotitem.count.WindowResultFunction;
import com.tqz.java.hotitem.entity.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;

/**
 * 需求：每隔5分钟输出最近一小时内点击量最多的前 N 个商品
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为了打印到控制台的结果不乱序，将配置全局的并发改为1，这里改变并发对结果正确性没有影响
        env.setParallelism(1);
        // 【重要】告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        /**
         * 开始加载本地数据源
         */
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

        /*
         * 1. 创建数据源，数据类型是 UserBehavior
         */
        DataStream<UserBehavior> dataSource = env.createInput(csvInput, pojoType);
        /*
         * flink 工作在 Event Time 的时候，每一个元素必须被分配一个时间戳。
         * 即使是数据自己携带了时间戳也要通过分配一次告知 flink
         *
         * 2. 获得业务时间，以及生成 Watermark
         */
        DataStream<UserBehavior> timeData = dataSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.timestamp * 1000; // 得到了一个带有时间标记的数据流了
            }
        });
        /*
         * 3. 过滤出点击事件 (因为只需要统计点击量)
         */
        DataStream<UserBehavior> pvData = timeData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return value.behavior.equals("pv");
            }
        });
        /*
         * 4. 设置窗口大小
         */
        DataStream<ItemViewCount> windowedData =
                pvData
                        .keyBy("itemId")
                        .timeWindow(Time.minutes(60), Time.minutes(5))
                        // 指定完窗口后，必须要指明在窗口上面的计算 WindowFunction 就是干这个的
                        .aggregate(new CountAgg(), new WindowResultFunction()); // 现在我们得到了每个商品在每个窗口的点击量的数据流。

        /*
         * 5. 为了统计每个窗口下最热门的商品，需要再次按窗口进行分组，这里根据ItemViewCount中的windowEnd进行keyBy()操作
         */
        DataStream<String> topItems = windowedData
                .keyBy("windowEnd")
                .process(new TopNHotItems(3));  // 求点击量前3名的商品

        topItems.print();
        env.execute("HotItems");
    }
}
