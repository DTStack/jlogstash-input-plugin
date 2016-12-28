package com.dtstack.logstash.distributed;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	
	private Set<Entry<String, List<String>>> routeRule;
	
	private String zkAddress;
	
	private String distributeRootNode;
	
	private String localAddress;
	
	private CuratorFramework zkClient;
	
	private InterProcessMutex lock;
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	@SuppressWarnings("unchecked")
	public ZkDistributed(Map<String,Object> distribute) throws Exception{
		this.distributed = distribute;
		checkDistributedConfig();
        if (StringUtils.isNotBlank(zkAddress)){
        	this.zkClient =createWithOptions(zkAddress,new ExponentialBackoffRetry(1000, 3), 1000, 1000);
        	this.zkClient.start();
        	this.lock = new InterProcessMutex(zkClient,String.format("%s/%s", this.distributeRootNode,"lock"));
        }
	}
    
	@SuppressWarnings("unchecked")
	private void checkDistributedConfig() throws Exception{
        this.routeRule  =((Map<String, List<String>>) this.distributed.get("routeRule")).entrySet();
        this.zkAddress = (String) distributed.get("zkAddress");
        String[] zks = this.zkAddress.split("/");
        if(StringUtils.isBlank(this.zkAddress)||zks.length<2){
        	throw new Exception("zkAddress is error");
        }
        this.zkAddress = zks[0].trim();
        this.distributeRootNode = String.format("/%s", zks[1].trim());
        this.localAddress  = (String) distributed.get("localAddress");
        if(StringUtils.isBlank(this.localAddress)){
        	throw new Exception("localAddress is error");
        }
	}
	
	private  CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) throws IOException {
		return CuratorFrameworkFactory.builder().connectString(connectionString)
				.retryPolicy(retryPolicy)
				.connectionTimeoutMs(connectionTimeoutMs)
				.sessionTimeoutMs(sessionTimeoutMs)
				.build();
	}
	
	public void zkRegistration() throws Exception{
		if(zkClient.checkExists().forPath(this.distributeRootNode)==null){
			try{
				zkClient.create().forPath(this.distributeRootNode);
			}catch(KeeperException.NodeExistsException e){
				logger.warn("%s node is Exist",this.distributeRootNode);
			}
		}
		String localNode = String.format("%s/%s", this.distributeRootNode,this.localAddress);
		Map<String,Long> nodeSign  = Maps.newHashMap();
		nodeSign.put("pn", 0l);
		nodeSign.put("bs", 0l);
		Stat stat = zkClient.checkExists().forPath(localNode);
		byte[] data = objectMapper.writeValueAsBytes(nodeSign);
		if(stat==null){
			zkClient.create().forPath(localNode,data);
		}else{
			zkClient.setData().forPath(localNode,data);
		}
	}
	
	public boolean originalRoute(Map<String,Object> event){
	  for(Entry<String, List<String>> entry:routeRule){
		  Object value = event.get(entry.getKey());
		  if(value!=null){
			  if(routeRule.contains(value))return false;
		  }
	  }
	  return true;
	}
	
	public void route(Map<String,Object> event){
		
	}
}

