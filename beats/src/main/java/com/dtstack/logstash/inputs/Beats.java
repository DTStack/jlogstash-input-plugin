package com.dtstack.logstash.inputs;

import io.netty.channel.ChannelHandlerContext;
import java.io.FileInputStream;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.annotation.Required;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:15:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Beats extends BaseInput {

	private final static Logger logger = LoggerFactory.getLogger(Beats.class);

	@Required(required = true)
	private static int port;

	private static String host = "0.0.0.0";

	private static String sslCertificate;

	private static String sslKey;

	private static String noPkcs7SslKey;

	private static boolean sslEnable = false;
	
	private Server server;
	
	private MessageListener messageListener;

	@SuppressWarnings("rawtypes")
	public Beats(Map config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void prepare() {
		// TODO Auto-generated method stub

	}

	@Override
	public void emit() {
		// TODO Auto-generated method stub
		try {
			messageListener = new MessageListener(this);
			server = new Server(host, port, messageListener);
			if (sslEnable) {
				PrivateKeyConverter converter = new PrivateKeyConverter(
						noPkcs7SslKey, null);
				logger.debug("SSLCertificate: {}", sslCertificate);
				logger.debug("SSLKey: {}", sslKey);
				// SslSimpleBuilder sslBuilder = new
				// SslSimpleBuilder(sslCertificate, sslKey, null)
				SslSimpleBuilder sslBuilder = new SslSimpleBuilder(
						new FileInputStream(sslCertificate),
						converter.convert(), null).setProtocols(
						new String[] { "TLSv1.2" }).setCertificateAuthorities(
						sslCertificate);
				server.enableSSL(sslBuilder);
			}
			server.listen();
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.exit(1);
		}
	}

	@Override
	public void release() {
		// TODO Auto-generated method stub
		try {
			if(server!=null)server.stop();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("beat server stop error: {}",e.getCause());
		}
	}

	public class MessageListener implements IMessageListener {

        private Beats beats;
		
		public MessageListener(Beats beats) {
			this.beats = beats;
		}

		@SuppressWarnings({ "unchecked"})
		@Override
		public void onNewMessage(ChannelHandlerContext ctx, Message message) {
			Map<String,Object> map =message.getData();
			System.out.println(map);
			if (map!=null){
				this.beats.process(map);
			}
		}

		@Override
		public void onNewConnection(ChannelHandlerContext ctx) {
		}

		@Override
		public void onConnectionClose(ChannelHandlerContext ctx) {
		}

		@Override
		public void onException(ChannelHandlerContext ctx,Throwable cause) {
               logger.debug("onException:{}",cause.getCause());
		}
	}
}

