package com.tqz.java.hotbooks;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * @Auther: qinzheng.tian
 * @Date: 2019/9/26 14:23
 * @Description:
 */
public class TopNAllFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {

    private int topSize = 3;

    public TopNAllFunction(int topSize) {
        this.topSize = topSize;
    }

    /**
     * 计算该窗口下的 Top N
     * Top N 的实现算法：小顶堆、红黑树
     *
     * @param context
     * @param elements
     * @param out
     * @throws Exception
     */
    @Override
    public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {

        TreeMap<Integer, Tuple2<String, Integer>> treeMap = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                // 默认升序，现在自定义为：降序
                return (o1 < o2) ? -1 : 1;
            }
        });

        // 1. 遍历窗口里面的数据，按照 Tuple2 的第二个参数，也就是出现的次数排序
        for (Tuple2<String, Integer> element : elements) {
            treeMap.put(element.f1, element);
            // 保留前 Top N 个元素
            if (treeMap.size() > topSize) {
                treeMap.pollLastEntry();
            }
        }

        // 2. 将红黑树里面的 Top N 个数据全部发射出去
        out.collect(
                "=================\n热销图书列表:\n"
                        + new Timestamp(System.currentTimeMillis()) + treeMap.toString()
                        + "\n===============\n"
        );
    }
}
