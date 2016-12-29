package com.dtstack.logstash.distributed;

import java.util.List;

import com.google.common.collect.Lists;


/**
 * 
 * @author sishu.yss
 *
 */
public class BrokerNode {
	
	private long timeStamp = System.currentTimeMillis();
	
	private List<String> metas = Lists.newArrayList();

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public List<String> getMetas() {
		return metas;
	}

	public void setMetas(List<String> metas) {
		this.metas = metas;
	}
	
}
