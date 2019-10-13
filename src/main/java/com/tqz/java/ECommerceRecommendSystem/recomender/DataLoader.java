package com.tqz.java.ECommerceRecommendSystem.recomender;

import com.tqz.java.ECommerceRecommendSystem.recomender.entity.Products;
import com.tqz.java.ECommerceRecommendSystem.recomender.entity.Rating;
import com.tqz.java.ECommerceRecommendSystem.recomender.sink.ProductsMongoOutputFormat;
import com.tqz.java.ECommerceRecommendSystem.recomender.sink.RatingsMongoOutputFormat;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DataLoader {
    private final static String PRODUCTS_PATH = "C:\\Users\\tqz\\IdeaProjects\\flink\\src\\main\\resources\\products.csv";
    private final static String RATINGS_PATH = "C:\\Users\\tqz\\IdeaProjects\\flink\\src\\main\\resources\\ratings.csv";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSet<String> data1 = env.readTextFile(PRODUCTS_PATH);
        DataSet<String> data2 = env.readTextFile(RATINGS_PATH);
        DataSet<Products> formatData1= data1.flatMap(new RichFlatMapFunction<String, Products>() {
            @Override
            public void flatMap(String in, Collector<Products> out) throws Exception {
                String[] str = in.split("\\^");
                Products products = new Products(Integer.valueOf(str[0]), str[1], str[4], str[5], str[6]);
                out.collect(products);
            }
        });
        DataSet<Rating> formatData2= data2.flatMap(new RichFlatMapFunction<String, Rating>() {
            @Override
            public void flatMap(String in, Collector<Rating> out) throws Exception {
                String[] str = in.split(",");
                Rating rating = new Rating(Integer.valueOf(str[0]), str[1], Double.valueOf(str[2]), Integer.valueOf(str[3]));
                out.collect(rating);
            }
        });

        formatData1.output(new ProductsMongoOutputFormat());
        formatData2.output(new RatingsMongoOutputFormat());

        env.execute();
    }
}
