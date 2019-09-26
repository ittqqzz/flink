package com.tqz.java.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: qinzheng.tian
 * @Date: 2019/9/26 09:49
 * @Description: WordCount SQL版本
 */
public class WCSQL {
    public static void main(String[] args) throws Exception {
        // 1. 获取 table 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        // 2. 获取数据
        List list = new ArrayList();
        String wordsStr = "1,2,1";
        String[] words = wordsStr.split(",");
        for (String word : words) {
            WCPojo wc = new WCPojo(word, 1);
            list.add(wc);
        }
        DataSet<WCPojo> input = env.fromCollection(list);
        // 3. 将 DataSet 转为 Table
        tEnv.registerDataSet("WCTable", input, "word,amount");
        // 4. 执行 sql（sql 必须将全部的字段全部查到并匹配。否则异常）
        // select count from WCTable 会出现异常
        Table table = tEnv.sqlQuery("SELECT word, SUM(amount) as amount FROM WCTable GROUP BY word");
        // 5. 将 table 转为 stream
        DataSet<WCPojo> result = tEnv.toDataSet(table, WCPojo.class);
        result.print();
    }

    // 类必须是 public
    public static class WCPojo {
        public String word;
        public int amount;

        // 必须有默认的无参构造方法
        public WCPojo() {
        }

        public WCPojo(String word, int amount) {
            this.word = word;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "word: " + word + " amount: " + amount;
        }
    }
}
