package com.dtstack.logstash.inputs;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.logstash.annotation.Required;
import com.dtstack.logstash.decoder.IDecode;
import com.dtstack.logstash.distributed.ZkDistributed;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月27日 下午1:16:06
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class KafkaDistributed extends BaseInput implements IKafkaChg{
	private static final Logger logger = LoggerFactory.getLogger(KafkaDistributed.class);

	private Map<String, ConsumerConnector> consumerConnMap = new HashMap<>();
	
	private Map<String, ExecutorService> executorMap = new HashMap<>();
	
	private  String encoding="UTF8";
	
	@Required(required=true)
	private  Map<String, Integer> topic;
	
	@Required(required=true)
	private  Map<String, String> consumerSettings;
	
	private ScheduledExecutorService scheduleExecutor;
	
	//是否开启partitions监控
	private  boolean openBalance = false;
	
	/**
	 *  是否开启分布式 null不开启，不为null开启
	 * 	{"zkAddress":"127.0.0.1:2181/distributed",
	 *   "routeRule":["logtype eq jvm1.8.log"]
	 *  }
	 */
	private  Map<String,String> distributed;
	
	private ZkDistributed zkDistributed;
	
	private  int consumerMoniPeriod =  3600 * 1000;
	
	private  int partitionsMoniPeriod = 10 * 1000;
	
	private ReentrantLock lock = new ReentrantLock();

	private class Consumer implements Runnable {
		private KafkaStream<byte[], byte[]> m_stream;
		private KafkaDistributed kafkaInput;
		private IDecode decoder;

		public Consumer(KafkaStream<byte[], byte[]> a_stream, KafkaDistributed kafkaInput) {
			this.m_stream = a_stream;
			this.kafkaInput = kafkaInput;
			this.decoder = kafkaInput.getDecoder();
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
							Map<String, Object> event = this.decoder
									.decode(m);
							if(zkDistributed==null||zkDistributed.originalRoute(event)){
								this.kafkaInput.process(event);
							}else{
								zkDistributed.route(event);
							}
						} catch (Exception e) {
							logger.error("process event:{} failed:{}",m,e.getCause());
						}
					}
				}
			} catch (Exception t) {
				logger.error("kakfa Consumer fetch is error:{}",t.getCause());
			}
		}
	}

	public KafkaDistributed(Map<String, Object> config){
		super(config);
	}

	@SuppressWarnings("unchecked")
	public void prepare() {
		Properties props = geneConsumerProp();
		
		for(String topicName : topic.keySet()){
			ConsumerConnector consumer = kafka.consumer.Consumer
					.createJavaConsumerConnector(new ConsumerConfig(props));
			
			consumerConnMap.put(topicName, consumer);
		}
		if(distributed!=null){
			zkDistributed = new ZkDistributed(distributed);
			zkDistributed.zkRegistration();
		}
	}
	
	private Properties geneConsumerProp(){
		Properties props = new Properties();

		Iterator<Entry<String, String>> consumerSetting = consumerSettings
				.entrySet().iterator();

		while (consumerSetting.hasNext()) {
			Map.Entry<String, String> entry = consumerSetting.next();
			String k = entry.getKey();
			String v = entry.getValue();
			props.put(k, v);
		}
		
		return props;
	}

	public void emit() {
		
		Iterator<Entry<String, Integer>> topicIT = topic.entrySet().iterator();
		while (topicIT.hasNext()) {
			
			Map.Entry<String, Integer> entry = topicIT.next();
			String topic = entry.getKey();
			Integer threads = entry.getValue();
			addNewConsumer(topic, threads);
		}
		
		//-----监控-----
		if(openBalance){
			startMonitor();
		}
	}
	
	public void addNewConsumer(String topic, Integer threads){
		ConsumerConnector consumer = consumerConnMap.get(topic);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = null;
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, threads);
		consumerMap = consumer.createMessageStreams(topicCountMap);
		
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		ExecutorService executor = Executors.newFixedThreadPool(threads);

		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new Consumer(stream, this));
		}
		
		executorMap.put(topic, executor);
	}

	@Override
	public void release() {
		
		for(ConsumerConnector consumer : consumerConnMap.values()){
			consumer.commitOffsets(true);
			consumer.shutdown();
		}
		
		for(ExecutorService executor : executorMap.values()){
			executor.shutdownNow();
		}
		
		scheduleExecutor.shutdownNow();
	}
	
	public void startMonitor(){
		
		logger.info("-----monitor kafka info(consumers,partitions,brokers)--------");
		String zookeeperConn = consumerSettings.get("zookeeper.connect");
		String groupId = consumerSettings.get("group.id");
		scheduleExecutor = Executors.newScheduledThreadPool(1);
		
		MonitorCluster monitor = new MonitorCluster(zookeeperConn, topic.keySet(), groupId, this, consumerMoniPeriod, partitionsMoniPeriod);
		scheduleExecutor.scheduleAtFixedRate(monitor, 10, 10, TimeUnit.SECONDS);
	}

	@Override
	public void onInfoChgTrigger(String topicName, int consumers, int partitions) {
		
		lock.lock();
		
		try{
			int expectNum = partitions/consumers;
			if(partitions%consumers > 0){
				expectNum++;
			}
			
			Integer threadNum = topic.get(topicName);
			if(threadNum == null){
				logger.error("invaid topic:{}.", topicName);
				return;
			}
			
			if(threadNum != expectNum){
				logger.warn("need chg thread num, curr threadNum:{}, expect threadNum:{}.", threadNum, expectNum);
				topic.put(topicName, expectNum);
				//停止,重启客户端
				reconnConsumer(topicName);
			}
		}catch(Exception e){
			logger.error("", e);
		}finally{
			lock.unlock();
		}
	}
	
	public void onClusterShutDown(){
		logger.error("---- kafka cluster shutdown!!!");
	}
	
	public void reconnConsumer(String topicName){
		
		//停止topic 对应的conn
		ConsumerConnector consumerConn = consumerConnMap.get(topicName);
		consumerConn.commitOffsets(true);
		consumerConn.shutdown();
		consumerConnMap.remove(topicName);
		
		//停止topic 对应的stream消耗线程
		ExecutorService es = executorMap.get(topicName);
		es.shutdownNow();	
		executorMap.remove(topicName);
		
		Properties prop = geneConsumerProp();
		ConsumerConnector newConsumerConn = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(prop));
		consumerConnMap.put(topicName, newConsumerConn);
		
		addNewConsumer(topicName, topic.get(topicName));
	}
}
