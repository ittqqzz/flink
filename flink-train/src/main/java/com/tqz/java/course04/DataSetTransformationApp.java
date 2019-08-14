package com.tqz.java.course04;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataSetTransformationApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        filterFunction(env);
//        mapPartitionFunction(env);
//        flatMapFunction(env);
//        distinctFunction(env);
//        joinFunction(env);
//        outerJoinFunction(env);
//        crossFunction(env);


    }

    // 求笛卡尔积
    public static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> info1 = new ArrayList<String>();
        info1.add("曼联");
        info1.add("费城");


        List<String> info2 = new ArrayList<String>();
        info2.add("3");
        info2.add("1");
        info2.add("0");

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<String> data2 = env.fromCollection(info2);

        data1.cross(data2).print();
    }

    public static void outerJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "PK哥"));
        info1.add(new Tuple2(2, "J哥"));
        info1.add(new Tuple2(3, "小队长"));
        info1.add(new Tuple2(4, "猪头呼"));


        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1, "北京"));
        info2.add(new Tuple2(2, "上海"));
        info2.add(new Tuple2(3, "成都"));
        info2.add(new Tuple2(5, "杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

//        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String, String>>() {
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                if(second == null) {
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
//
//                } else {
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
//                }
//            }
//        }).print();

//        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String, String>>() {
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                if(first == null) {
//                    return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
//                } else {
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
//                }
//            }
//        }).print();


        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
                } else if (second == null) {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
                } else {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                }
            }
        }).print();

    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "PK哥"));
        info1.add(new Tuple2(2, "J哥"));
        info1.add(new Tuple2(3, "小队长"));
        info1.add(new Tuple2(4, "猪头呼"));


        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1, "北京"));
        info2.add(new Tuple2(2, "上海"));
        info2.add(new Tuple2(3, "成都"));
        info2.add(new Tuple2(5, "杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.join(data2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                    }
                }).print();
    }

    public static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<>();
        list.add("hadoop,spark");
        list.add("hadoop,flin");
        list.add("flin,flin");
        env.fromCollection(list).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String in, Collector<String> out) throws Exception {
                String[] tokens = in.split(",");
                for (String token : tokens) {
                    out.collect(token);
                }
            }
        }).distinct().print();
    }

    public static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<>();
        list.add("hadoop,spark");
        list.add("hadoop,flin");
        list.add("flin,flin");
        env.fromCollection(list).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String in, Collector<String> out) throws Exception {
                String[] tokens = in.split(",");
                for (String token : tokens) {
                    out.collect(token);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        }).groupBy(0).sum(1).print();
    }


    public static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        // 设置并行度位 3 的话，partition就会分成三个区
        DataSet<Integer> data = env.fromCollection(list).setParallelism(3);
//        data.map(new MapFunction<Integer, Integer>() {
//            @Override
//            public Integer map(Integer in) throws Exception {
//                System.out.println("获取连接");
//
//                System.out.println("关闭连接");
//                return in + 1;
//            }
//        }).print();

        data.mapPartition(new MapPartitionFunction<Integer, Integer>() {
            @Override
            public void mapPartition(Iterable<Integer> in, Collector<Integer> out) throws Exception {
                System.out.println("获取连接");

                System.out.println("关闭连接");
            }
        }).print();
    }

    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        DataSet<Integer> data = env.fromCollection(list);
        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer in) throws Exception {
                return in + 1;
            }
        }).print();
    }

    public static void filterFunction(ExecutionEnvironment env) throws Exception {
        env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9))
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value > 5;
                    }
                }).print();
    }
}
