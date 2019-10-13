package com.tqz.java.hotbooks;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Auther: qinzheng.tian
 * @Date: 2019/9/26 13:30
 * @Description: 自定义的 source 需要实现 SourceFunction
 */
public class MyNoParalleSource implements SourceFunction<String> {

    private boolean isRunning = true;

    private int count = 1;

    /**
     * 死循环，不断往目的地 sink 数据
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        List<String> books = new ArrayList<>();
        books.add("Python 从入门到放弃");
        books.add("Java 从入门到放弃");
        books.add("Php 从入门到放弃");
        books.add("C++ 从入门到放弃");
        books.add("Scala 从入门到放弃");
        while (isRunning) {
            int i = new Random().nextInt(5);
            ctx.collect(books.get(i));
            if (count == 1) {
                System.out.println("==========");
            }
            System.out.println(books.get(i));
            if (count == 4) {
                count = 0;
                System.out.println("==========");
            }
            count += 1;
            Thread.sleep(1000);
        }
    }

    // 取消一个cancel的时候会调用的方法
    @Override
    public void cancel() {
        isRunning = false;
    }
}

