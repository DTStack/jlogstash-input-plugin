package com.dtstack.logstash.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.exception.ExceptionUtil;

public class UpReblance implements Runnable{
	
	private static final Logger logger = LoggerFactory.getLogger(HeartBeatCheck.class);

	private final static int INTERVAL = 10*60*1000;
	
	private ZkDistributed zkDistributed;
	
	private MasterCheck masterCheck;
	
    public UpReblance(ZkDistributed zkDistributed, MasterCheck masterCheck){
    	this.zkDistributed = zkDistributed;
    	this.masterCheck = masterCheck;
    }
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			Thread.sleep(INTERVAL);
			if(this.masterCheck.isMaster()){
				this.zkDistributed.upReblance();
			}
		}catch(Exception e){
			logger.error(ExceptionUtil.getErrorMessage(e));
		}
	}
}
