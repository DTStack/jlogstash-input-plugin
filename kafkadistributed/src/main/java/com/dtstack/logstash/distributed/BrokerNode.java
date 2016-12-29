package com.dtstack.logstash.distributed;

import java.util.List;

import com.google.common.collect.Lists;


/**
 * 
 * @author sishu.yss
 *
 */
public class BrokerNode {
	
	private long seq = 0;
	
	private  boolean alive = true;
	
	private List<String> metas = Lists.newArrayList();
	
	public boolean isAlive() {
		return alive;
	}

	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	public long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
	}

	public List<String> getMetas() {
		return metas;
	}

	public void setMetas(List<String> metas) {
		this.metas = metas;
	}
}
