package com.dtstack.logstash.distributed;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.exception.ExceptionUtil;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月28日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class MasterCheck implements Runnable{
	
	private static final Logger logger = LoggerFactory.getLogger(MasterCheck.class);

    private AtomicBoolean isMaster = new AtomicBoolean(false);
    
	private ZkDistributed zkDistributed;
	
	private final static int MASTERCHECK = 1000;

    public MasterCheck(ZkDistributed zkDistributed){
    	this.zkDistributed = zkDistributed;
    }
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			isMaster.getAndSet(zkDistributed.setMaster());
			Thread.sleep(MASTERCHECK);
		}catch(Exception e){
			logger.error("MasterCheck error:{}",ExceptionUtil.getErrorMessage(e));
		}
	}

	public boolean isMaster() {
		return isMaster.get();
	}
}
