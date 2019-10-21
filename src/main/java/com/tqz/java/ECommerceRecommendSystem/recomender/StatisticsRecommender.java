package com.tqz.java.ECommerceRecommendSystem.recomender;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.mapred.JobConf;

import javax.xml.crypto.Data;


/**
 * 离线推荐
 * 离线推荐服务主要分为
 * - 统计推荐、
 * - 基于隐语义模型的协同过滤推荐
 * - 以及基于内容和基于Item-CF的相似推荐
 */
public class StatisticsRecommender {

//    private static String MONGODB_RATING_COLLECTION = "Rating";
//    //统计的表的名称
//    private static String RATE_MORE_PRODUCTS = "RateMoreProducts";
//    private static String RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts";
//    private static String AVERAGE_PRODUCTS = "AverageProducts";
//
//    public static final String MONGO_URI = "mongodb://120.79.241.167:27017/recommender.Rating";

    public static void main(String[] args) throws Exception {
        // 加载 mongodb 的数据
        //创建运行环境
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        //将mongo数据转化为Hadoop数据格式
//        HadoopInputFormat<BSONWritable, BSONWritable> hdIf =
//                new HadoopInputFormat<>(new MongoInputFormat(), BSONWritable.class,
//                        BSONWritable.class, new JobConf());
//        hdIf.getJobConf().set("mongo.input.uri", MONGO_URI);
//        DataSet<Tuple2<BSONWritable, BSONWritable>> input = env.createInput(hdIf);

/**
 * 加载 MongoDB 的事以后再说
 */

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
        String PATH = "C:\\Users\\tqz\\IdeaProjects\\flink\\src\\main\\resources\\ratings.csv";
        DataSet<Rating> csv = env.readCsvFile(PATH).pojoType(Rating.class, "userId", "productId",
                "score", "timestamp");

        // 数据源转为 table
        Table ratingTable = tableEnv.fromDataSet(csv);
        tableEnv.registerTable("ratings", ratingTable);

        // 使用 flink 的 sql 执行统计，视频： 10/29
        // 历史热门商品统计
        Table resultTable = tableEnv.sqlQuery("SELECT productId, count(productId) AS countNum from ratings GROUP BY productId");
        DataSet<Row> res = tableEnv.toDataSet(resultTable, Row.class);
        res.print();

    }

    public static class Rating {
        public Integer userId;
        public String productId;
        public Double score;
        public Integer timestamp;
    }
}
