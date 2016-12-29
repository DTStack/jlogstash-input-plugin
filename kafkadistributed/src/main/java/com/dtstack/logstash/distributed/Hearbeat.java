package com.dtstack.logstash.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.exception.ExceptionUtil;



/**
 * 
 * @author sishu.yss
 *
 */
public class Hearbeat implements Runnable{

	private static final Logger logger = LoggerFactory.getLogger(Hearbeat.class);

	private final static int HEATBEAT = 1000;
	
	private ZkDistributed zkDistributed;
	
	public Hearbeat(ZkDistributed zkDistributed){
		this.zkDistributed  = zkDistributed;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			zkDistributed.updateLocalNode(false);
			zkDistributed.updateMemBrokersNodeData();
			Thread.sleep(HEATBEAT);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Hearbeat fail:{}",ExceptionUtil.getErrorMessage(e));
		}
	}
}
