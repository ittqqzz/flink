package com.tqz.java.es;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

public class ESStudy {

    public final static String HOST = "120.79.241.167"; //本地服务器部署

    public final static int PORT = 9300; //http请求的端口是9200，客户端是9300

    private static TransportClient client = null; // 客户端连接

    public static void getConnect() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "my-es-application").build();
        client = new PreBuiltTransportClient(settings).addTransportAddresses(
                new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));
        System.out.println("=======\nES 连接信息: " + client.toString() + " \n=======");
    }

    public static void closeConnect() {
        if (null != client) {
            System.out.println("======= 执行关闭连接操作... =======");
            client.close();
        }
    }

    /**
     * 创建索引
     *
     * @throws IOException
     */
    public static void addIndex() throws IOException {
        /*
         *建立文档对象
         * 参数一 blog1：表示索引对象
         * 参数二 article：类型
         * 参数三 1：建立id
         */
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 1)
                .field("title", "elasticSearch搜索引擎")
                .field("content",
                        "全文搜索服务器")
                .endObject();

        IndexResponse indexResponse = client.prepareIndex("blog", "article", Integer.toString(1)).setSource(builder).get();

        // 结果获取
        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String id = indexResponse.getId();
        long version = indexResponse.getVersion();
        RestStatus status = indexResponse.status();
        System.out.println("index索引：" + index + "--类型：" + type + "--索引id：" + id + "--版本：" + version + "--状态：" + status);
    }

    /**
     * 删除索引下的文档
     * 只删除指定 id 的数据
     *
     * @throws Exception
     */
    public static void deleteByDocId() throws Exception {
        ActionFuture<DeleteResponse> res = client.delete(new DeleteRequest("blog", "article", "1"));
        System.out.println("======= 删除结果 =======");
        System.out.println("======= " + "状态：" + res.actionGet().status() + " =======");
    }

    /**
     * 删除索引以及全部数据
     *
     * @param indexName
     */
    public static void deleteByIndexName(String indexName) {
        DeleteIndexResponse res = client.admin().indices().prepareDelete(indexName)
                .execute().actionGet();
        System.out.println("======= 删除结果 =======");
        System.out.println("======= " + "状态：" + res.isAcknowledged() + " =======");
    }

    /**
     * 查询
     *
     * @throws Exception
     */
    public static void getIndexNoMapping() throws Exception {
        GetResponse actionGet = client.prepareGet("blog", "article", "1").execute().actionGet();
        System.out.println("=======\n查询结果：" + actionGet.getSourceAsString());
    }

    // 查询所有文档数据
    public static void getMatchAll() throws IOException {

        // get() === execute().actionGet()
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article").setQuery(QueryBuilders.matchAllQuery())
                .get();
        // 获取命中次数，查询结果有多少对象
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            // 每个查询对象
            SearchHit searchHit = iterator.next();
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            System.out.println("title:" + searchHit.getSource().get("title"));
        }
    }

    // 关键字查询
    public static void getKeyWord() throws IOException {
        long time1 = System.currentTimeMillis();
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article").setQuery(QueryBuilders.queryStringQuery("elasticSearch"))
                .get();
        // 获取命中次数，查询结果有多少对象
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            // 每个查询对象
            SearchHit searchHit = iterator.next();
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            System.out.println("title:" + searchHit.getSource().get("title"));
        }
        long time2 = System.currentTimeMillis();
        System.out.println("花费" + (time2 - time1) + "毫秒");
    }

    // 通配符查询
    public static void getByLike() throws IOException {
        long time1 = System.currentTimeMillis();
        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article").setQuery(QueryBuilders.wildcardQuery("desc", "可爱*")) //通配符查询
                .setTypes("article").setQuery(QueryBuilders.wildcardQuery("content", "服务器"))
//                .setTypes("article").setQuery(QueryBuilders.termQuery("content","全文")) //词条查询
                //一般情况下只显示十条数据
                //from + size must be less than or equal to: [10000]
                //Scroll Search->支持1万以上的数据量
                // .setSize(10000)
                .get();
        // 获取命中次数，查询结果有多少对象
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            // 每个查询对象
            SearchHit searchHit = iterator.next();
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            System.out.println("title:" + searchHit.getSource().get("title"));
        }
        long time2 = System.currentTimeMillis();
        System.out.println("花费" + (time2 - time1) + "毫秒");
    }

    // 聚合查询
    public static void combinationQuery() throws Exception {
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("title", "搜索"))// 词条查询
                        //.must(QueryBuilders.rangeQuery("id").from(1).to(5))  // 范围查询
                        //因为IK分词器，在存储的时候将英文都变成了小写
                        .must(QueryBuilders.wildcardQuery("content", "Rest*".toLowerCase())) // 模糊查询
                        .must(QueryBuilders.queryStringQuery("服电风扇丰盛的分器")) // 关键字（含有）
                )
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("总记录数：" + hits.getTotalHits());
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            System.out.println(searchHit.getSourceAsString());
            Map<String, Object> source = searchHit.getSource();
            System.out.println(source.get("id")+"    "+source.get("title")+"    "+source.get("content"));
        }

    }

    public static void main(String[] args) throws Exception {
        getConnect();

//        addIndex();
//        deleteByDocId();
//        deleteByIndexName("blog");
//        getIndexNoMapping();
//        getMatchAll();
//        getKeyWord();

//        getByLike();
        combinationQuery();

        closeConnect();
    }
}
