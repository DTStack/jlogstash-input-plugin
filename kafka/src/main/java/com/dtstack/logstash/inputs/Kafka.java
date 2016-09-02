package com.dtstack.logstash.inputs;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.logstash.annotation.Required;
import com.dtstack.logstash.assembly.InputQueueList;
import com.dtstack.logstash.decoder.IDecode;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:16:06
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class Kafka extends BaseInput {
	private static final Logger logger = LoggerFactory.getLogger(Kafka.class);

	private ConsumerConnector consumer;
	
	private ExecutorService executor;
	
	private static String encoding="UTF8";
	
	@Required(required=true)
	private static Map<String, Integer> topic;
	
	@Required(required=true)
	private static Map<String, String> consumerSettings;

	private class Consumer implements Runnable {
		private KafkaStream<byte[], byte[]> m_stream;
		private Kafka kafkaInput;
		private IDecode decoder;

		public Consumer(KafkaStream<byte[], byte[]> a_stream, Kafka kafkaInput) {
			this.m_stream = a_stream;
			this.kafkaInput = kafkaInput;
			this.decoder = kafkaInput.createDecoder();
		}

		public void run() {
			try {
				while(true){
					ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
					while (it.hasNext()) {
						String m = null;
						try {
							m = new String(it.next().message(),
									this.kafkaInput.encoding);
						} catch (UnsupportedEncodingException e1) {
							e1.printStackTrace();
							logger.error(e1.getMessage());
						}

						try {
							Map<String, Object> event = this.decoder
									.decode(m);
							this.kafkaInput.inputQueueList.put(event);
						} catch (Exception e) {
							logger.error("process event failed:" + m);
							e.printStackTrace();
							logger.error(e.getMessage());
						}
					}
				}
			} catch (Exception t) {
				logger.error("kakfa Consumer fetch is error",t);
			}
		}
	}

	public Kafka(Map<String, Object> config,InputQueueList inputQueueList){
		super(config,inputQueueList);
	}

	@SuppressWarnings("unchecked")
	public void prepare() {
		Properties props = new Properties();

		Iterator<Entry<String, String>> consumerSetting = consumerSettings
				.entrySet().iterator();

		while (consumerSetting.hasNext()) {
			Map.Entry<String, String> entry = consumerSetting.next();
			String k = entry.getKey();
			String v = entry.getValue();
			props.put(k, v);
		}
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(props));
	}

	public void emit() {
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = null;
		consumerMap = consumer.createMessageStreams(topic);
		Iterator<Entry<String, Integer>> topicIT = topic.entrySet().iterator();
		while (topicIT.hasNext()) {
			Map.Entry<String, Integer> entry = topicIT.next();
			String topic = entry.getKey();
			Integer threads = entry.getValue();
			List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

			executor = Executors.newFixedThreadPool(threads);

			for (final KafkaStream<byte[], byte[]> stream : streams) {
				executor.submit(new Consumer(stream, this));
			}
		}
	}

	@Override
	public void release() {
		// TODO Auto-generated method stub
		consumer.commitOffsets(true);
		consumer.shutdown();
		executor.shutdownNow();
	}
}
