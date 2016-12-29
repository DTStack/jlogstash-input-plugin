package com.dtstack.logstash.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.exception.ExceptionUtil;

/**
 * 
 * @author sishu.yss
 *
 */
public class MonitorJlogstashCluster implements Runnable{
	
	private static final Logger logger = LoggerFactory.getLogger(MonitorJlogstashCluster.class);
	
	private ZkDistributed zkDistributed =null;
	
	public MonitorJlogstashCluster(ZkDistributed zkDistributed){
		this.zkDistributed = zkDistributed;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			if(zkDistributed.setMaster()){
				
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(ExceptionUtil.getErrorMessage(e));
		}
	}
	
	private void reblance(){
		
	}
}
