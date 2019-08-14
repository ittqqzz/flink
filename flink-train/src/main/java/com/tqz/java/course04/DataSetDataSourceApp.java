package com.tqz.java.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.List;

public class DataSetDataSourceApp {
    public static final String PATH = "D:\\MyConfiguration\\qinzheng.tian\\IdeaProjects\\flink\\input\\";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollection(env);
//        fromTextFile(env);
//        fromCSVFile(env);
//        fromRecursive(env);
        fromCompressed(env);
    }

    public static void fromCompressed(ExecutionEnvironment env) throws Exception {
        env.readTextFile(PATH + "\\hello.txt.gz").print();
    }

    public static void fromRecursive(ExecutionEnvironment env) throws Exception {
        // 递归读取文件需要设置 parameters
        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);
        env.readTextFile(PATH)
                .withParameters(parameters)
                .print();
    }

    public static class User {
        private String name;
        private String sex;
        private Integer age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", sex='" + sex + '\'' +
                    ", age=" + age +
                    '}';
        }
    }

    public static void fromCSVFile(ExecutionEnvironment env) throws Exception {
        env.readCsvFile(PATH + "\\csv.csv")
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .includeFields("011")
                .pojoType(User.class, "sex", "age").print();

    }

    public static void fromTextFile(ExecutionEnvironment env) throws Exception {
        // 可以读取文件或者是文件夹
        env.readTextFile(PATH).print();
    }

    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        env.fromCollection(list).print();
    }
}
