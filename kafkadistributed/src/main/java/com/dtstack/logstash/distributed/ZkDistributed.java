package com.dtstack.logstash.distributed;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.retry.ExponentialBackoffRetry;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月27日 下午3:16:06
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class ZkDistributed {
	
	private static final Logger logger = LoggerFactory.getLogger(ZkDistributed.class);
	
	private  Map<String,Object> distributed;
	
//	private Set<Entry<String, List<String>>> routeRule;
	
	private String zkAddress;
	
	private String distributeRootNode;
	
	private String localAddress;
	
	private String localNode;
	
	private String brokersNode;

	private CuratorFramework zkClient;
	
	private InterProcessMutex createNodelock;
	
	private String hashKey;
	
	private Map<String,BrokerNode> nodeDatas = Maps.newConcurrentMap();
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	private static ZkDistributed zkDistributed;
	
    private RouteSelect routeSelect;
	
	
	public static synchronized ZkDistributed getSingleZkDistributed(Map<String,Object> distribute) throws Exception{
		if(zkDistributed!=null)return zkDistributed;
		zkDistributed = new ZkDistributed(distribute);
		return zkDistributed;
	}
	
	@SuppressWarnings("unchecked")
	public ZkDistributed(Map<String,Object> distribute) throws Exception{
		this.distributed = distribute;
		checkDistributedConfig();
        if (StringUtils.isNotBlank(zkAddress)){
        	this.zkClient =createWithOptions(zkAddress,new ExponentialBackoffRetry(1000, 3), 1000, 1000);
        	this.zkClient.start();
        	this.createNodelock = new InterProcessMutex(zkClient,String.format("%s/%s", this.distributeRootNode,"lock"));
        }
        routeSelect = new RouteSelect(this);
	}
    
	@SuppressWarnings("unchecked")
	private void checkDistributedConfig() throws Exception{
//        this.routeRule  =((Map<String, List<String>>) this.distributed.get("routeRule")).entrySet();
        this.zkAddress = (String) distributed.get("zkAddress");
        if(StringUtils.isBlank(this.zkAddress)||this.zkAddress.split("/").length<2){
        	throw new Exception("zkAddress is error");
        }
        String[] zks = this.zkAddress.split("/");
        this.zkAddress = zks[0].trim();
        this.distributeRootNode = String.format("/%s", zks[1].trim());
        this.localAddress  = (String) distributed.get("localAddress");
        if(StringUtils.isBlank(this.localAddress)){
        	throw new Exception("localAddress is error");
        }
        this.hashKey  = (String) distributed.get("hashKey");
        if(StringUtils.isBlank(this.hashKey)||this.hashKey.split(":").length<2){
        	throw new Exception("hashKey is error");
        }
        this.brokersNode = String.format("%s/brokers", this.distributeRootNode);
        this.localNode = String.format("%s/%s", this.brokersNode,this.localAddress);
	}
	
	private  CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) throws IOException {
		return CuratorFrameworkFactory.builder().connectString(connectionString)
				.retryPolicy(retryPolicy)
				.connectionTimeoutMs(connectionTimeoutMs)
				.sessionTimeoutMs(sessionTimeoutMs)
				.build();
	}
	
	public void zkRegistration() throws Exception{
		createNodeIfNotExists(this.distributeRootNode);
		createNodeIfNotExists(this.brokersNode);
		Stat stat = zkClient.checkExists().forPath(localNode);
		if(stat==null){
			createLocalNode();
		}else{
			updateLocalNode(true);
		}
		updateMemBrokersNodeData();
	}
	
   public void createNodeIfNotExists(String node) throws Exception{
		if(zkClient.checkExists().forPath(node)==null){
			try{
				zkClient.create().forPath(node);
			}catch(KeeperException.NodeExistsException e){
				logger.warn("%s node is Exist",node);
			}
		}
   }
	
   @SuppressWarnings("unchecked")
  public synchronized void updateMemBrokersNodeData() throws Exception{
	  List<String> childrens =  zkClient.getChildren().forPath(this.brokersNode);
      if(childrens!=null){
    	  for(String node:childrens){
    		  BrokerNode data = objectMapper.readValue(zkClient.getData().forPath(String.format("%s/%s", this.brokersNode,node)), BrokerNode.class);
    		  nodeDatas.put(node, data);
    	  }
      }
      Set<Map.Entry<String, BrokerNode>> sets = nodeDatas.entrySet();
      for(Map.Entry<String, BrokerNode> entry:sets){
    	 Long t =  (Long) entry.getValue().getTimeStamp();
    	 if(System.currentTimeMillis()-t > Hearbeat.EXPIRED){
    		 nodeDatas.remove(entry.getKey());
    	 }
      }
   }
	
    public void createLocalNode() throws Exception{
		byte[] data = objectMapper.writeValueAsBytes(new BrokerNode());
		zkClient.create().forPath(localNode,data);
    }
    
    @SuppressWarnings("unchecked")
	public void updateLocalNode(boolean cover) throws Exception{
    	BrokerNode nodeSign = objectMapper.readValue(zkClient.getData().forPath(localNode), BrokerNode.class);
    	nodeSign.setTimeStamp(System.currentTimeMillis());
    	if(cover) nodeSign.setMetas(Lists.newArrayList());
    	byte[] data = objectMapper.writeValueAsBytes(nodeSign);
		zkClient.setData().forPath(localNode,data);
    }
    
    public void updateBrokerNodeMeta(String node,String sign) throws Exception{
    	String nodePath = String.format("%s/%s", this.brokersNode,node);
    	BrokerNode nodeSign = objectMapper.readValue(zkClient.getData().forPath(nodePath), BrokerNode.class);
    	nodeSign.getMetas().add(sign);
    	nodeSign.setTimeStamp(System.currentTimeMillis());
    	zkClient.setData().forPath(nodePath,objectMapper.writeValueAsBytes(nodeSign));
    	updateMemBrokersNodeData();
    }

	public RouteSelect getRouteSelect() {
		return routeSelect;
	}

	public InterProcessMutex getCreateNodelock() {
		return createNodelock;
	}

	public String getHashKey() {
		return hashKey;
	}

	public Map<String, BrokerNode> getNodeDatas() {
		return nodeDatas;
	}

	public String getLocalAddress() {
		return localAddress;
	}
}

