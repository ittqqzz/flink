package com.tqz.java.ECommerceRecommendSystem.recomender.sink;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.tqz.java.ECommerceRecommendSystem.recomender.entity.Rating;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.bson.Document;

import java.io.IOException;

public class RateMoreProductsMongoOutputFormat implements OutputFormat<Row> {

    static MongoClient mongoClient = null;
    static MongoDatabase mongoDatabase = null;

    @Override
    public void configure(Configuration parameters) {
        System.out.println("mongoSink 配置中");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        System.out.println("连接到 mongoDB ");
        // 连接到 mongodb 服务
        mongoClient = new MongoClient("120.79.241.167", 27017);
        // 连接到数据库
        mongoDatabase = mongoClient.getDatabase("recommender");

        System.out.println("Connect to database successfully");
    }

    @Override
    public void writeRecord(Row row) throws IOException {

        MongoCollection<Document> collection = mongoDatabase.getCollection("RateMoreProducts");
        //插入文档
        Document document = new Document("productId", row.getField(0)).
                append("countNum", row.getField(1));
        collection.insertOne(document);
        System.out.println("文档插入成功");
    }

    @Override
    public void close() throws IOException {
        System.out.println("关闭 mongo 连接");
        mongoClient.close();
    }
}
