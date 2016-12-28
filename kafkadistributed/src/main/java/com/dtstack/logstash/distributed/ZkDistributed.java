package com.dtstack.logstash.distributed;

import java.util.Map;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月27日 下午3:16:06
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class ZkDistributed {
	
	private  Map<String,String> distributed;
	
	public ZkDistributed(Map<String,String> distribute){
		this.distributed = distribute;
	}

	public boolean originalRoute(Map<String,Object> event){
	  return true;
	}
	
	public Map<String, String> getDistributed() {
		return distributed;
	}
}
