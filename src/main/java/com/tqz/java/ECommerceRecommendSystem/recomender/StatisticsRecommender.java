package com.tqz.java.ECommerceRecommendSystem.recomender;

import com.tqz.java.ECommerceRecommendSystem.recomender.sink.AverageProductsMongoOutputFormat;
import com.tqz.java.ECommerceRecommendSystem.recomender.sink.RateMoreProductsMongoOutputFormat;
import com.tqz.java.ECommerceRecommendSystem.recomender.sink.RateMoreRecentlyProductsMongoOutputFormat;
import lombok.Data;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.util.Date;


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
        env.setParallelism(1);
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
        String PATH = "C:\\Users\\tqz\\IdeaProjects\\flink\\src\\main\\resources\\ratings.csv";
        DataSet<Rating> csv = env.readCsvFile(PATH).pojoType(Rating.class, "userId", "productId", "score", "timestamp");

        // 一、历史热门商品统计：就是统计商品ID出现的次数，然后降序排列
        Table table1 = tableEnv.fromDataSet(csv);
        tableEnv.registerTable("ratings", table1);
        Table resultTable1 = tableEnv.sqlQuery("SELECT productId, count(productId) AS countNum from ratings GROUP BY productId order by countNum desc");
        DataSet<Row> res1 = tableEnv.toDataSet(resultTable1, Row.class);
        res1.output(new RateMoreProductsMongoOutputFormat());

        // 二、最近热门商品统计
        // 将时间戳改为年月格 1260759144000  => 201605，然后按照时间分组求出每个月的热门商品
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMM");
        MapOperator<Rating, Rating> formatCsv = csv.map(new RichMapFunction<Rating, Rating>() {
            @Override
            public Rating map(Rating value) throws Exception {
                String formatTime = simpleDateFormat.format(new Date(value.timestamp * 1000L));
                value.setYearmonth(Integer.valueOf(formatTime));
                return value;
            }
        });
        Table table2 = tableEnv.fromDataSet(formatCsv);
        tableEnv.registerTable("ratingOfMonth", table2);
        Table resultTable2 =  tableEnv.sqlQuery("select productId, count(productId) as countNum ,yearmonth from ratingOfMonth group by yearmonth,productId order by yearmonth desc, countNum desc");
        DataSet<Row> res2 = tableEnv.toDataSet(resultTable2, Row.class);
        res2.output(new RateMoreRecentlyProductsMongoOutputFormat());

        // 三、商品平均得分统计：根据历史数据中所有用户对商品的评分，周期性的计算每个商品的平均得分
        // 借助一里面的表执行 sql
        Table resultTable3 =  tableEnv.sqlQuery("select productId, avg(score) as scoreAvg from ratings group by productId order by scoreAvg desc");
        DataSet<Row> res3 = tableEnv.toDataSet(resultTable3, Row.class);
        res3.output(new AverageProductsMongoOutputFormat());

        env.execute("StatisticsRecommender");
    }

    @Data
    public static class Rating {
        public Integer userId;
        public String productId;
        public Double score;
        public Integer timestamp;
        public Integer yearmonth;

    }
}
