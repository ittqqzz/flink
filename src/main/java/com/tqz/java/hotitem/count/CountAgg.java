package com.tqz.java.hotitem.count;

import com.tqz.java.hotitem.entity.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * COUNT 统计的聚合函数实现，每出现一条记录加一
 *
 * AggregateFunction 是 Reduce 的广义版本，将进入窗口的元素逐渐聚合起来，三个参数依次为 IN、ACC、OUT
 */
public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long acc) {
        return acc + 1;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc1, Long acc2) {
        return acc1 + acc2;
    }
}
