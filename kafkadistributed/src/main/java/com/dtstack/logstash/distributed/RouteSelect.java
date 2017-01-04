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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.exception.ExceptionUtil;
import com.dtstack.logstash.netty.client.NettySend;
import com.dtstack.logstash.render.Formatter;
import com.google.common.collect.Maps;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月28日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class RouteSelect {
	
	private static final Logger logger = LoggerFactory.getLogger(RouteSelect.class);
	
	private Map<String,NettySend> nettySends = Maps.newConcurrentMap();
	
	private  ZkDistributed zkDistributed = null;
	
	private String keyPrefix;
	
	private String keyHashCode;

	public RouteSelect(ZkDistributed zkDistributed,String hashKey){
		this.zkDistributed = zkDistributed;
		String[] ks= hashKey.split(":");
		keyPrefix = ks[0];
		keyHashCode = ks[1];
	}

	public void route(Map<String,Object> event) throws Exception{
		String prefix = Formatter.format(event,keyPrefix);
		int hashcode  = Formatter.format(event,keyHashCode).hashCode();
		String sign  = String.format("%s_%d", prefix,hashcode);
		String broker = getBroker(sign);
		NettySend nettySend = null;
		if(broker!=null){ 
			nettySend = getNettySend(broker);
		}else{
			try{
				zkDistributed.getAddMetaToNodelock().acquire();
				zkDistributed.updateMemBrokersNodeData();
				broker = getBroker(sign);
				if(broker!=null){
					nettySend = getNettySend(broker);
				}else{
					broker = selectRoute();
					zkDistributed.updateBrokerNodeMeta(broker, sign);
					nettySend = getNettySend(broker);
				}
			}catch(Exception e){
				logger.error(ExceptionUtil.getErrorMessage(e));
			}finally{
				zkDistributed.getAddMetaToNodelock().release();
			}
		}
		nettySend.emit(event);
	}
	
	public String selectRoute(){
		 Set<Entry<String, BrokerNode>> nodeDatas  = zkDistributed.getNodeDatas().entrySet();
		 int i = Integer.MAX_VALUE;
		 String route = null;
		 for(Entry<String, BrokerNode> node:nodeDatas){
			List<String> metaData =  node.getValue().getMetas();
			int size = metaData.size();
			if(i>size) {
				i =size;
				route = node.getKey();
			}
		 }
		 return route==null?zkDistributed.getLocalAddress():route;
	}
	
	private NettySend getNettySend(String broker){
		NettySend nettySend = nettySends.get(broker);
		if(nettySend==null){
			nettySend = new NettySend(broker);
			nettySends.put(broker, nettySend);
		}
		return nettySend;
	}
	
	public String getBroker(String sign){
	      Set<Map.Entry<String,BrokerNode>> sets = zkDistributed.getNodeDatas().entrySet();
	      for(Map.Entry<String, BrokerNode> entry:sets){
	    	  List<String> ps =entry.getValue().getMetas();
	    	  if(ps.contains(sign)){
	    		  return entry.getKey();
	    	  }
	      }
		return null;
	}
}
