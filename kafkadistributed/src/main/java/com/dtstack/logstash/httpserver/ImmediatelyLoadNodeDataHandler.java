package com.dtstack.logstash.httpserver;

import java.io.IOException;
import java.io.OutputStream;

import com.dtstack.logstash.distributed.ZkDistributed;
import com.sun.net.httpserver.HttpExchange;


/**
 * 
 * @author sishu.yss
 *
 */
public class ImmediatelyLoadNodeDataHandler extends PostHandler{
	
	
	private ZkDistributed zkDistributed;
	
	public ImmediatelyLoadNodeDataHandler(ZkDistributed zkDistributed) {
		// TODO Auto-generated constructor stub
		this.zkDistributed = zkDistributed;
	}

	@Override
	public void handle(HttpExchange he) throws IOException {
		// TODO Auto-generated method stub
		 OutputStream os = he.getResponseBody();
	}
	
}
