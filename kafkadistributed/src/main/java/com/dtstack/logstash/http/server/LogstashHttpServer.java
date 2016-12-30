package com.dtstack.logstash.http.server;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.distributed.ZkDistributed;
import com.sun.net.httpserver.HttpServer;

/**
 * 
 * @author sishu.yss
 *
 */
@SuppressWarnings("restriction")
public class LogstashHttpServer {
	
	private static final Logger logger = LoggerFactory.getLogger(LogstashHttpServer.class);

	private String host="0.0.0.0";
	
	private int port;
	
	private ZkDistributed zkDistributed;
	
	private HttpServer server;
	
	public LogstashHttpServer(ZkDistributed zkDistributed,String localAddress) throws Exception{
		this.zkDistributed = zkDistributed;
		String[] la = localAddress.split(":");
		this.port = Integer.parseInt(la[1])+1;
		init();
	}
	
	private void init() throws Exception{
		this.server = HttpServer.create(new InetSocketAddress(InetAddress.getByAddress(host.getBytes()),port), 0);
		server.setExecutor(null);
		setHandler();
		this.server.start();
		logger.warn("LogstashHttpServer start at:{}",String.valueOf(port));
	}
	
	private void setHandler(){
		this.server.createContext("/loadNodeData",new ImmediatelyLoadNodeDataHandler(this.zkDistributed));
	}
}
