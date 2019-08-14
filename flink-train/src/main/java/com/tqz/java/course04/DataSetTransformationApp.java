package com.tqz.java.course04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class DataSetTransformationApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        mapFunction(env);
    }

    public static void mapFunction(ExecutionEnvironment env) throws Exception{
        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9);
        DataSet<Integer>  data = env.fromCollection(list);
        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer in) throws Exception {
                return in + 1;
            }
        }).print();
    }
}
