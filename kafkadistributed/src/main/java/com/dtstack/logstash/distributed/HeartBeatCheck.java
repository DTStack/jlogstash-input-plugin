package com.dtstack.logstash.distributed;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.exception.ExceptionUtil;
import com.google.common.collect.Maps;

/**
 * 
 * @author sishu.yss
 *
 */
public class HeartBeatCheck implements Runnable{
	
	private static final Logger logger = LoggerFactory.getLogger(HeartBeatCheck.class);
	
	private ZkDistributed zkDistributed;
	
	private final static int HEATBEATCHECK = 2000;
	
	private final static int EXCEEDCOUNT = 3;
	
	private MasterCheck masterCheck;

	public HeartBeatCheck(ZkDistributed zkDistributed,MasterCheck masterCheck){
		this.zkDistributed = zkDistributed;
		this.masterCheck = masterCheck;
	}
	
	public Map<String,BrokerNodeCount> brokerNodeCounts =  Maps.newConcurrentMap();
	
	
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			if(this.masterCheck.isMaster()){
				healthCheck();
			}
			Thread.sleep(HEATBEATCHECK);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(ExceptionUtil.getErrorMessage(e));
		}
	}
	
	private void healthCheck(){
		List<String> childrens = zkDistributed.getBrokersChildren();
		if(childrens!=null){
			for(String node:childrens){
				BrokerNode brokerNode = zkDistributed.getBrokerNodeData(node);
				if(brokerNode!=null&&brokerNode.isAlive()){
					BrokerNodeCount brokerNodeCount = brokerNodeCounts.get(node);
					if(brokerNodeCount==null){
						brokerNodeCount = new BrokerNodeCount(0,brokerNode);
					}
					if(brokerNodeCount.getBrokerNode().getSeq()==brokerNode.getSeq()){
						brokerNodeCount.setCount(brokerNodeCount.getCount()+1);
					}else{
						brokerNodeCount.setCount(0);
					}
					if(brokerNodeCount.getCount() > EXCEEDCOUNT){//node died
						brokerNode.setAlive(false);
						this.zkDistributed.updateBrokerNode(node, brokerNode);
						brokerNodeCounts.remove(node);
					}else{
						brokerNodeCount.setBrokerNode(brokerNode);
						brokerNodeCounts.put(node, brokerNodeCount);
					}
				}
			}
		}
	}
	
	static class BrokerNodeCount{
		
		private int count;
		
		private BrokerNode brokerNode;
		
		public BrokerNodeCount(int count,BrokerNode brokerNode){
			this.count = count;
			this.brokerNode  = brokerNode;
		}

		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
		public BrokerNode getBrokerNode() {
			return brokerNode;
		}
		public void setBrokerNode(BrokerNode brokerNode) {
			this.brokerNode = brokerNode;
		}
	}
}
