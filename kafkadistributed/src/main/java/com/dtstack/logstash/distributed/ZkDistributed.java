/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.logstash.distributed;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.exception.ExceptionUtil;
import com.dtstack.logstash.http.cilent.LogstashHttpClient;
import com.dtstack.logstash.http.server.LogstashHttpServer;
import com.dtstack.logstash.logmerge.LogPool;
import com.dtstack.logstash.netty.server.NettyRev;
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
	
	private String zkAddress;
	
	private String distributeRootNode;
	
	private String localAddress;
	
	private String localNode;
	
	private String brokersNode;

	private CuratorFramework zkClient;
	
	private InterProcessMutex addMetaToNodelock;
	
	private InterProcessMutex updateNodelock;
	
	private InterProcessMutex masterlock;
	
	private String hashKey;
	
	private Map<String,BrokerNode> nodeDatas = Maps.newConcurrentMap();
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	private static ZkDistributed zkDistributed;
	
    private RouteSelect routeSelect;
    
    private LogstashHttpClient logstashHttpClient;
    
    private LogstashHttpServer logstashHttpServer;
    
    private LogPool logPool;
    
	private ExecutorService executors;
	
	private NettyRev nettyRev;

    
	public static synchronized ZkDistributed getSingleZkDistributed(Map<String,Object> distribute) throws Exception{
		if(zkDistributed!=null)return zkDistributed;
		zkDistributed = new ZkDistributed(distribute);
		return zkDistributed;
	}
	
	public ZkDistributed(Map<String,Object> distribute) throws Exception{
		this.distributed = distribute;
		checkDistributedConfig();
		initZk();
        this.addMetaToNodelock = new InterProcessMutex(zkClient,String.format("%s/%s", this.distributeRootNode,"addMetaToNodelock"));
        this.masterlock = new InterProcessMutex(zkClient,String.format("%s/%s", this.distributeRootNode,"masterlock"));
        this.updateNodelock = new InterProcessMutex(zkClient,String.format("%s/%s", this.distributeRootNode,"updateNodelock"));
        this.nettyRev = new NettyRev(this.localAddress);
        this.nettyRev.startup();
        logPool = LogPool.getInstance();
        this.routeSelect = new RouteSelect(this,this.hashKey);
        this.logstashHttpServer = new LogstashHttpServer(zkDistributed);
        this.logstashHttpClient = new LogstashHttpClient(zkDistributed);
        initScheduledExecutorService();
	}
    
	private void initZk() throws IOException{
        this.zkClient =createWithOptions(zkAddress,new ExponentialBackoffRetry(1000, 3), 1000, 1000);
        this.zkClient.start();
	}
	
	private void initScheduledExecutorService(){
		executors = Executors.newFixedThreadPool(5);
		MasterCheck masterCheck = new MasterCheck(zkDistributed);
		executors.submit(new HearBeat(zkDistributed));
		executors.submit(masterCheck);
		executors.submit(new HeartBeatCheck(zkDistributed,masterCheck));
		executors.submit(new DownReblance(zkDistributed,masterCheck));
		executors.submit(new UpReblance(zkDistributed,masterCheck));
	}
	
	private void checkDistributedConfig() throws Exception{
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
			this.updateBrokerNode(this.localAddress, BrokerNode.initBrokerNode());
		}
		updateMemBrokersNodeData();
		setMaster();
	}
	
   public boolean setMaster(){
	   boolean flag =false;
	   try{
			String master = isHaveMaster();
			if(this.localAddress.equals(master))return true;
			this.masterlock.acquire(30,TimeUnit.SECONDS);
			master = isHaveMaster();
            if(master==null||!getBrokerNodeData(master).isAlive()){
          	   this.zkClient.setData().forPath(this.brokersNode, this.localAddress.getBytes());
          	   flag = true ;
            }
		}catch(Exception e){
			logger.error(ExceptionUtil.getErrorMessage(e));
		}finally{
			try{
				this.masterlock.release();
			}catch(Exception e){
				logger.error(ExceptionUtil.getErrorMessage(e));
			}
		}
		return flag;
   }	
	
	
   public String isHaveMaster() throws Exception{
	   byte[] data = this.zkClient.getData().forPath(this.brokersNode);
	   if(data==null||StringUtils.isBlank(objectMapper.readValue(data, BrokersNode.class).getMaster())){
		   return null;
	   }
	   return objectMapper.readValue(data, BrokersNode.class).getMaster();
   }	
   
	
   public void createNodeIfNotExists(String node) throws Exception{
		if(zkClient.checkExists().forPath(node)==null){
			try{
				zkClient.create().forPath(node,objectMapper.writeValueAsBytes(new BrokersNode()));
			}catch(KeeperException.NodeExistsException e){
				logger.warn("%s node is Exist",node);
			}
		}
   }
	
  public synchronized void updateMemBrokersNodeData() throws Exception{
	  List<String> childrens = getBrokersChildren();
      if(childrens!=null){
    	  for(String node:childrens){
    		  BrokerNode data = objectMapper.readValue(zkClient.getData().forPath(String.format("%s/%s", this.brokersNode,node)), BrokerNode.class);
    		  nodeDatas.put(node, data);
    	  }
      }
      Set<Map.Entry<String, BrokerNode>> sets = nodeDatas.entrySet();
      for(Map.Entry<String, BrokerNode> entry:sets){
    	 if(!entry.getValue().isAlive()){
     		 nodeDatas.remove(entry.getKey());
    	 }
      }
   }
	
    public void createLocalNode() throws Exception{
		zkClient.create().forPath(localNode,objectMapper.writeValueAsBytes(BrokerNode.initBrokerNode()));
    }

	public void disableLocalNode(String node){
		BrokerNode brokerNode = BrokerNode.initNullBrokerNode();
		brokerNode.setAlive(false);
		updateBrokerNode(node,brokerNode);
	}
		
	public void updateBrokerNode(String node,BrokerNode nodeSign){
		try{
			this.updateNodelock.acquire(30, TimeUnit.SECONDS);
	    	String nodePath = String.format("%s/%s", this.brokersNode,node);
	    	BrokerNode brokerNode = this.getBrokerNodeData(node);
	    	BrokerNode.copy(nodeSign, brokerNode);
	    	zkClient.setData().forPath(nodePath,objectMapper.writeValueAsBytes(brokerNode));
		}catch(Exception e){
			logger.error("{}:updateBrokerNode error:{}",node,ExceptionUtil.getErrorMessage(e));
		}finally{
			try{
				this.updateNodelock.release();
			}catch(Exception e){
				logger.error("{}:updateBrokerNode error:{}",node,ExceptionUtil.getErrorMessage(e));
			}
		}
	}
	
    
    public void updateBrokerNodeMeta(String node,String sign) throws Exception{
    	BrokerNode nodeSign = getBrokerNodeData(node);
    	if(nodeSign!=null){
        	nodeSign.getMetas().add(sign);
        	updateBrokerNode(node,nodeSign);
        	updateMemBrokersNodeData();
    	}
    }
    
    public List<String> getBrokersChildren(){
    	try{
        	return zkClient.getChildren().forPath(this.brokersNode);
    	}catch(Exception e){
    		logger.error("getBrokersChildren error:{}",ExceptionUtil.getErrorMessage(e));
    	}
    	return null;
    }
    
    public BrokerNode getBrokerNodeData(String node){
    	try{
        	String nodePath = String.format("%s/%s", this.brokersNode,node);
        	BrokerNode nodeSign = objectMapper.readValue(zkClient.getData().forPath(nodePath), BrokerNode.class);
        	return nodeSign;
    	}catch(Exception e){
    		logger.error("{}:getBrokerNodeData error:{}",node,ExceptionUtil.getErrorMessage(e));
    	}
    	return null;
    }
    
	public InterProcessMutex getAddMetaToNodelock() {
		return addMetaToNodelock;
	}

	public Map<String, BrokerNode> getNodeDatas() {
		return nodeDatas;
	}

	public String getLocalAddress() {
		return localAddress;
	}

	public void route(Map<String, Object> event) throws Exception {
		// TODO Auto-generated method stub
		this.routeSelect.route(event);
	}
	
	public void route(List<Map<String, Object>> events) throws Exception {
		// TODO Auto-generated method stub
		if(events != null){
			for(Map<String, Object> event:events){
				this.routeSelect.route(event);
			}
		}
	}
	
	
	public void realse() throws Exception{
		disableLocalNode(this.localAddress);
		downReblance();
	}
	
	public void downReblance() throws Exception{
		 BrokerNode brokerNode = BrokerNode.initBrokerNode();
		 Map<String,BrokerNode> nodes = Maps.newConcurrentMap();
		 List<String> childrens = this.getBrokersChildren();
		 List<String> failNodes = Lists.newArrayList();
		 for(String child:childrens){
			 BrokerNode bb= this.getBrokerNodeData(child);
			 if(!bb.isAlive()&&bb.getMetas().size()>0){
				 brokerNode.getMetas().addAll(bb.getMetas());
				 failNodes.add(child);
			 }else{
				 nodes.put(child, bb);
			 }
		 }
		 if(brokerNode.getMetas().size() > 0){
			 int total = brokerNode.getMetas().size();
		     List<Map.Entry<String,BrokerNode>> entries = new LinkedList<Map.Entry<String,BrokerNode>>(nodes.entrySet());

		        Collections.sort(entries, new Comparator<Map.Entry<String,BrokerNode>>() {

		            @Override
		            public int compare(Map.Entry<String, BrokerNode> o1, Map.Entry<String, BrokerNode> o2) {
		                return o1.getValue().getMetas().size()-o2.getValue().getMetas().size();
		            }
		        });
		     
			 
			 for(Map.Entry<String, BrokerNode> entry:entries){
				 total = total+ entry.getValue().getMetas().size();
			 }
			 int avg = total/nodes.size();
			 int start =0;
			 int end = 0;
			 int resultTotal = 0;
		     for(Map.Entry<String, BrokerNode> entry:entries){
		    	 List<String> metas = entry.getValue().getMetas();
		    	 int msize = metas.size();
		    	 if(msize<avg){
		    		 end = end+(avg - msize);
			    	 metas.addAll(brokerNode.getMetas().subList(start, end));
			    	 start = end;
			    	 resultTotal = resultTotal+ metas.size();
			    	 continue;
		    	 }
		    	 resultTotal = resultTotal+ metas.size();
		     }	
		     if(total>resultTotal){
		    	 int c = total - resultTotal;
		    	 int index = 0;
			     for(Map.Entry<String, BrokerNode> entry:entries){
			    	 if(index<c){
				    	 end = end+1;
				    	 entry.getValue().getMetas().addAll(brokerNode.getMetas().subList(start, end));
				    	 start = end;
			    	     index++;
			    	 }
			     }
		     } 
		    for(Map.Entry<String,BrokerNode> entry:entries){
				BrokerNode nodeSign = BrokerNode.initNullBrokerNode();
				nodeSign.setMetas(entry.getValue().getMetas());
				this.updateBrokerNode(entry.getKey(), nodeSign);
			} 
		    for(String failNode:failNodes){
				BrokerNode nodeSign = BrokerNode.initNullBrokerNode();
				nodeSign.setMetas(Lists.newArrayList());
				this.updateBrokerNode(failNode, nodeSign);
			} 
			this.updateMemBrokersNodeData();
			logstashHttpClient.sendImmediatelyLoadNodeData();
			sendLogPoolData();
		 }
	}
	
	public void upReblance() throws Exception{
		 List<String> childrens = this.getBrokersChildren();
		 List<String> noneNode = Lists.newArrayList();
		 List<String> allNode = Lists.newArrayList();
		 List<String> nodes = Lists.newArrayList();
		 Map<String,BrokerNode> mnodes = Maps.newConcurrentMap();
		 for(String child:childrens){
			BrokerNode brokerNode = this.getBrokerNodeData(child);
			if(brokerNode.isAlive()){
				if(brokerNode.getMetas().size()>0){
					nodes.addAll(brokerNode.getMetas());
				}else{
					noneNode.add(child);
				}
				allNode.add(child);
			} 
		 }
		 int avg = nodes.size()/allNode.size();
		 int yu  = nodes.size()%allNode.size();
		 int start = 0;
		 int end = 0;
		 if(noneNode.size()>0){
			 for(int i=0;i < allNode.size();i++){
				 if(i==allNode.size()-1){
					 end = end + yu;
				 }else{
					 end = end +avg;
				 }
				 BrokerNode brokerNode = BrokerNode.initNullBrokerNode();
				 brokerNode.setMetas(nodes.subList(start, end));
				 mnodes.put(allNode.get(i), brokerNode);
				 start = end;
			 }
		 }
		 
		for(Map.Entry<String,BrokerNode> entry:mnodes.entrySet()){
				this.updateBrokerNode(entry.getKey(), entry.getValue());
	    } 
		this.updateMemBrokersNodeData();
		logstashHttpClient.sendImmediatelyLoadNodeData();
		sendLogPoolData();
		logstashHttpClient.sendImmediatelyLogPoolData();
	}
	
	public void sendLogPoolData() throws Exception{
		List<Map<String,Object>> events = this.logPool.getNotCompleteLog();
		route(events);
	}
}

