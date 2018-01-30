package elasticsearch;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

public class ElasticSearchTest {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchTest.class);
	//服务器地址
	private final static String HOST = "127.0.0.1";
	//端口号，http请求端口为9200，api调用为9300
	private final static int PORT = 9300;

	private TransportClient client = null;

	/**
	 * 
	* @Title: getConnection 
	* @Description: 获得elasticsearch客户连接
	* @param:     
	* @return void    
	* @throws
	 */
	@Before
	public void getConnection() {
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddress(new TransportAddress(new InetSocketAddress(HOST, PORT)));
		logger.info("连接信息：{}", client.toString());
	}

	/**
	 * 
	* @Title: closeConnection 
	* @Description: 关闭elasticsearch客户端连接
	* @param:     
	* @return void    
	* @throws
	 */
	@After
	public void closeConnection() {
		if (null != client) {
			logger.info("执行关闭操作");
			client.close();
		}
	}

	/**
	 * 
	* @Title: test1 
	* @Description: 从集群中获得连接
	* @param:     
	* @return void    
	* @throws
	 */
//	@Test
//	public void test1() {
//		try { // 设置集群名称 Settings settings =
//			Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build(); //
//			// 创建client
//			TransportClient client = new PreBuiltTransportClient(settings)
//					.addTransportAddress(new TransportAddress(new InetSocketAddress("127.0.0.1", 9300))); // 搜索数据accounts/person/1
//			GetResponse response = client.prepareGet("accounts", "person", "1").execute().actionGet(); // 输出结果
//			System.out.println(response.getSourceAsString());
//			client.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
	
	/**
	 * 
	* @Title: addIndex 
	* @Description: 添加索引，通过elasticsearch自带的XContentFactory
	* @param:     
	* @return void    
	* @throws
	 */
	@Test
	public void addIndex() {
		try {
			IndexResponse response;
			response = client
					.prepareIndex("msg", "tweet", "1").setSource(XContentFactory.jsonBuilder().startObject()
							.field("username", "张三").field("sendDate", new Date()).field("msg", "你好张三").endObject())
					.get();
			logger.info("索引名称：{}\n类型：{}\n文档id：{}\n当前实例状态：{}", response.getIndex(), response.getType(), response.getId(),
					response.status());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	/**
	 * 
	* @Title: addIndex2 
	* @Description: 添加document，通过Map的形式
	* @param:     
	* @return void    
	* @throws
	 */
	@Test
	public void addIndex2() {
		Map<String, Object> map = new HashMap<>();
		map.put("username", "李四");
		map.put("sendDate", new Date());
		map.put("msg", "你好李四");
		IndexResponse response = client.prepareIndex("msg", "tweet", "2").setSource(map).get();
		logger.info("索引名称：{}\n类型：{}\n文档id：{}\n当前实例状态：{}", response.getIndex(), response.getType(), response.getId(),
				response.status());
	}
	
	/**
	 * 
	* @Title: addIndex3 
	* @Description: 添加document，通过Gson的jsonObject
	* @param:     
	* @return void    
	* @throws
	 */
	@Test
	public void addIndex3() {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("username", "王五");
		jsonObject.addProperty("sendDate", "2018-01-30");
		jsonObject.addProperty("msg", "你好王五");

		IndexResponse response = client.prepareIndex("msg", "tweet", "3").setSource(jsonObject, XContentType.JSON).get();
		logger.info("索引名称：{}\n类型：{}\n文档id：{}\n当前实例状态：{}", response.getIndex(), response.getType(), response.getId(),
				response.status());
	}
	
	/**
	 * 
	* @Title: getData1 
	* @Description: 查询指定document
	* @param:     
	* @return void    
	* @throws
	 */
	@Test
	public void getData1() {
		GetResponse response = client.prepareGet("msg", "tweet", "1").get();
		logger.info("索引库的数据：{}", response.getSourceAsString());
	}
	
	/**
	 * 
	* @Title: updateData 
	* @Description: 更新指定document
	* @param:     
	* @return void    
	* @throws
	 */
	@Test
	public void updateData() {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("username", "谢健");
		jsonObject.addProperty("sendDate", "2018-01-30");
		jsonObject.addProperty("msg", "HI,谢健");
		
		UpdateResponse response = client.prepareUpdate("msg", "tweet", "1").setDoc(jsonObject.toString(), XContentType.JSON).get();
		logger.info("索引名称：{}\n类型：{}\n文档id：{}\n当前实例状态：{}", response.getIndex(), response.getType(), response.getId(),
				response.status());
	}
	
	/**
	 * 
	* @Title: deleteData 
	* @Description: 删除指定document 
	* @param:     
	* @return void    
	* @throws
	 */
	@Test
	public void deleteData() {
		DeleteResponse response = client.prepareDelete("msg", "tweet", "3").get();
		logger.info("索引名称：{}\n类型：{}\n文档id：{}\n当前实例状态：{}", response.getIndex(), response.getType(), response.getId(),
				response.status());
	}
		
}
