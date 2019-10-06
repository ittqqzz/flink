package com.tqz.java.es;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

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
     * 删除索引
     *
     * @throws Exception
     */
    public static void deleteByObject() throws Exception {
        ActionFuture<DeleteResponse> res =  client.delete(new DeleteRequest("blog", "article", "1"));
        System.out.println("======= 删除结果 =======");
        System.out.println("======= " + "状态：" + res.actionGet().status() + " =======");
    }

    public static void main(String[] args) throws Exception {
        getConnect();

        //addIndex();
        deleteByObject();

        closeConnect();
    }
}
