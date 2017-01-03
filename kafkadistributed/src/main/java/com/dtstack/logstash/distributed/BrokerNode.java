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

import java.util.List;

import com.google.common.collect.Lists;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月28日 下午1:16:37
 * Company: www.dtstack.com
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
