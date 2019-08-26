package com.tqz.java.practice;

import com.tqz.java.practice.count.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

    private final int topSize;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
    private ListState<ItemViewCount> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 状态的注册
        ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
                "itemState-state",
                ItemViewCount.class);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    /**
     * 每当收到一条数据（ItemViewCount），我们就注册一个 windowEnd+1 的定时器
     * windowEnd+1 的定时器被触发时，意味着收到了 windowEnd+1 的 Watermark，即收齐了该 windowEnd 下的所有商品窗口统计值
     * @param input
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(
            ItemViewCount input,
            Context context,
            Collector<String> collector) throws Exception {

        // 每条数据都保存到状态中
        itemState.add(input);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于 windowEnd 窗口的所有商品数据
        context.timerService().registerEventTimeTimer(input.windowEnd + 1);
    }

    /**
     * 然后在 onTimer() 中处理将收集的所有商品及点击量进行排序，选出 TopN，并将排名信息格式化成字符串后进行输出。
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(
            long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 获取收到的所有商品点击量
        List<ItemViewCount> allItems = new ArrayList<>();
        for (ItemViewCount item : itemState.get()) {
            allItems.add(item);
        }
        // 提前清除状态中的数据，释放空间
        itemState.clear();
        // 按照点击量从大到小排序
        allItems.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int) (o2.viewCount - o1.viewCount);
            }
        });
        // 将排名信息格式化成 String, 便于打印
        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < topSize; i++) {
            ItemViewCount currentItem = allItems.get(i);
            // No1:  商品ID=12224  浏览量=2413
            result.append("No").append(i).append(":")
                    .append("  商品ID = ").append(currentItem.itemId)
                    .append("  浏览量 = ").append(currentItem.viewCount)
                    .append("\n");
        }
        result.append("====================================\n\n");

        out.collect(result.toString());
    }
}
