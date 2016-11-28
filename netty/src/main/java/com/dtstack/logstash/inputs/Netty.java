package com.dtstack.logstash.inputs;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.annotation.Required;
import com.dtstack.logstash.assembly.InputQueueList;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Netty extends BaseInput {

	private static Logger logger = LoggerFactory.getLogger(Netty.class);

	@Required(required = true)
	private static int port;

	private static String host = "0.0.0.0";

	private static String encoding = "utf-8";
	
	private static int receiveBufferSize = 1024 * 1024 * 20;// 设置缓存区大小20M

	private static String delimiter = System.getProperty("line.separator");
	
	private static String multilineDelimiter = (char)29 +"";
	
	private ServerBootstrap bootstrap;
	
	private Executor bossExecutor;
	
	private Executor workerExecutor;

	public Netty(Map config, InputQueueList inputQueueList) {
		super(config, inputQueueList);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void prepare() {
		// TODO Auto-generated method stub
	}

	@Override
	public void release() {
		// TODO Auto-generated method stub
		if(bootstrap!=null)bootstrap.shutdown();
	}

	@Override
	public void emit() {
		// TODO Auto-generated method stub
		// Server服务启动器
		try {
			 bossExecutor = Executors.newCachedThreadPool();
			 workerExecutor = Executors.newCachedThreadPool();
			 bootstrap = new ServerBootstrap(
					new NioServerSocketChannelFactory(
							bossExecutor,workerExecutor));
			final NettyServerHandler nettyServerHandler = new NettyServerHandler(
					this);
			// 设置一个处理客户端消息和各种消息事件的类(Handler)
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				@Override
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline pipeline = Channels.pipeline();
					pipeline.addLast(
							"decoder",
							new DelimiterBasedFrameDecoder(Integer.MAX_VALUE,
									false, true, ChannelBuffers.copiedBuffer(
											delimiter,
											Charset.forName(encoding))));
					pipeline.addLast("handler", nettyServerHandler);
					return pipeline;
				}
			});
			bootstrap.setOption("child.receiveBufferSize", receiveBufferSize); 
			bootstrap.setOption("child.keepAlive", true);
			bootstrap.setOption("child.tcpNoDelay", true);
			bootstrap.bind(new InetSocketAddress(InetAddress.getByName(host),
					port));
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.exit(1);
		}
	}

	private static class NettyServerHandler extends SimpleChannelHandler {

		private Netty netty;

		public NettyServerHandler(Netty netty) {
			this.netty = netty;
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
				throws Exception {
			Object message = e.getMessage();
			if (message != null) {
				if (message instanceof ChannelBuffer) {
					String mes = ((ChannelBuffer) message).toString(Charset
							.forName(encoding));
					if (StringUtils.isNotBlank(mes)) {
						mes = multilineDecoder(mes);
						this.netty.process(this.netty.decoder.decode(mes));
					}
				}
			}
		}
		
		@Override
	    public void exceptionCaught(
	            ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		        logger.debug("netty io error:", e.getCause());
			    ctx.sendUpstream(e);
	    }	
		
		public String multilineDecoder(String msg){
			return msg.replace(multilineDelimiter, delimiter);
		}
	}
	
	public static void main(String[] args) {
		Map<String, Object> map = new HashMap<String, Object>();
		Netty netty = new Netty(map, null);
		netty.port = 9111;
		netty.emit();
	}
	
}
