package com.dtstack.logstash.inputs;

public interface IKafkaChg {
	
	/**
	 * 某个topic对应的分区发生变化,或者消耗的客户端数量发生变化
	 * @param topicName
	 * @param consumers
	 * @param partitions
	 */
	public void onInfoChgTrigger(String topicName, int consumers, int partitions);
	
	/**
	 * kafka集群挂掉
	 */
	public void onClusterShutDown();

}
