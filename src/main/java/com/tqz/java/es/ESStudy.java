package com.tqz.java.es;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

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
     * 创建索引并赋初值
     *
     * @throws IOException
     */
    public static void addIndexByBuilder() throws IOException {
        /*
         *建立文档对象
         * 参数一 blog1：表示索引对象
         * 参数二 article：类型
         * 参数三 1：建立id
         */
        XContentBuilder builder = jsonBuilder()
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

    // 通过 json 创建索引并赋初值
    public static void addIndexByJson() {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user", "kimchy5");
        json.put("postDate", new Date());
        json.put("message", "trying out Elasticsearch");
        // 第一个参数：索引名；
        // 第二个参数：索引类型；
        // 第三个参数：索引ID(相同的id时修改数据，默认为随机字符串)
        IndexResponse indexResponse = client.prepareIndex("twitter", "json", "1").setSource(json).get();

        // 结果获取
        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String id = indexResponse.getId();
        long version = indexResponse.getVersion();
        RestStatus status = indexResponse.status();
        System.out.println("index索引：" + index + "--类型：" + type + "--索引id：" + id + "--版本：" + version + "--状态：" + status);
    }

    public static void addIndexData() {
        // 批量插入数据，删除和修改也可以
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        Map<String, Object> json2 = new HashMap<String, Object>();
        json2.put("user", "Tony");
        json2.put("postDate", new Date());
        json2.put("message", "Elasticsearch is awesome");
        Map<String, Object> json3 = new HashMap<String, Object>();
        json3.put("user", "Piter");
        json3.put("postDate", new Date());
        json3.put("message", "全文搜索引擎");
//        IndexRequest request = client.prepareIndex("twitter", "tweet", "2").setSource(json2).request();
//        IndexRequest request2 = client.prepareIndex("twitter", "tweet", "3").setSource(json3).request();
        IndexRequest request = client.prepareIndex("twitter", "tweet").setSource(json2).request();
        IndexRequest request2 = client.prepareIndex("twitter", "tweet").setSource(json3).request();
        bulkRequest.add(request);
        bulkRequest.add(request2);

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        System.out.println("=======\n批量添加状态：" + bulkResponse.status());
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
            System.out.println(source.get("id") + "    " + source.get("title") + "    " + source.get("content"));
        }

    }

    // 更新索引数据
    public static void updateIndexData() throws Exception {
        //更新索引（根据索引，类型，id）
        UpdateRequest updateRequest = new UpdateRequest("twitter", "json", "1")
                .doc(jsonBuilder().startObject().field("message", "如果我说爱我没有如果").endObject());
        UpdateResponse response = client.update(updateRequest).get();
        System.out.println("=======\n更新结果：" + response.status());
    }

    // 插入更新：更新时数据不存在就插入
    public static void updateIndexDataOrAdd() throws Exception {
        // 如果不存在 id 为 4 的则插入 message 为 IndexRequest 的内容
        // 如果存在则更新 message 为 UpdateRequest 的内容
        IndexRequest indexRequest = new IndexRequest("twitter", "tweet", "4")
                .source(jsonBuilder().startObject()
                        .field("message", "每当我迷失在黑夜里")
                        .endObject());
        UpdateRequest updateRequest2 = new UpdateRequest("twitter", "tweet", "4")
                .doc(jsonBuilder().startObject()
                        .field("message", "请照亮我前行")
                        .endObject())
                .upsert(indexRequest);
        UpdateResponse response = client.update(updateRequest2).get();
        System.out.println("=======\n更新结果：" + response.status());

    }

    public static void main(String[] args) throws Exception {
        getConnect();

//        addIndexByBuilder();
//        addIndexByJson();
//        addIndexData();
//        deleteByDocId();
//        deleteByIndexName("blog");
//        getIndexNoMapping();
//        getMatchAll();
//        getKeyWord();

//        getByLike();
//        combinationQuery();

//        updateIndexData();
        updateIndexDataOrAdd();


        closeConnect();
    }
}
