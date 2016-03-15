package io.moquette.spi.impl.kafka;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.spi.impl.ProtocolProcessor;
import io.moquette.spi.impl.subscriptions.Subscription;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;

public class TopicCounsumerGroup {
	
	private static final Logger LOG = LoggerFactory.getLogger(TopicCounsumerGroup.class);

	private Set<Subscription> subscriptions = new HashSet<Subscription>();

	private ConsumerConnector consumer;

	private String topicFilter;

	private ProtocolProcessor protocolProcessor;

	private final String groupId;
	
	public TopicCounsumerGroup(
			ProtocolProcessor protocolProcessor,
			Properties props, 
			String topicFilter) {
		
		LOG.debug("New TopicConsumerGroup: " + topicFilter +" (group.id="+props.getProperty("group.id")+")");
		
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		
		this.topicFilter = topicFilter;
		this.protocolProcessor = protocolProcessor;
		this.groupId = props.getProperty("group.id");
	}
	
	public synchronized void init(int numThreads) {
		LOG.debug("Initializing...");
		
		List<KafkaStream<byte[], byte[]>> streams = 
				consumer.createMessageStreamsByFilter(
				new Whitelist(toKafkaTopicFilter(topicFilter)), 
				numThreads);
		
		int threadIdx = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			TopicConsumer consumer = new TopicConsumer(protocolProcessor, 
					stream, 
					topicFilter+"-"+threadIdx);
			consumer.setPriority(Thread.MAX_PRIORITY);
			consumer.start();
		}
		
        //Let consumer threads start
        //TODO ugly way, use locks
        try {
			Thread.sleep(500);
		} catch (InterruptedException e) {}
	}

	public static String toKafkaTopicFilter(String mqttTopicFilter) {
		if(mqttTopicFilter.equals("#"))
			return ".*";
		
		return mqttTopicFilter
				.replaceAll("/", ".")
				.replaceAll("\\+", "[^.]+")
				.replaceAll("#", "*");
	}

	public synchronized void subscribe(Subscription newSubscription) {
		LOG.debug("TopicConsumerGroup: " + topicFilter + " => New Subscription: " + newSubscription);
		
		subscriptions.add(newSubscription);
	}

	public synchronized void unsubscribe(String clientIDToRemove) {
		LOG.debug("TopicConsumerGroup: " + topicFilter + " => Removing Subscriptions for: " + clientIDToRemove);
		
		Iterator<Subscription> it = subscriptions.iterator();
		while (it.hasNext()) {
			Subscription subscription = (Subscription) it.next();
			
			if(subscription.getClientId().equals(clientIDToRemove)){
				it.remove();
				
				LOG.debug("TopicConsumerGroup: " + topicFilter + " => Removed Subscription: " + subscription);
			}
		}
	}

	public synchronized Set<Subscription> getSubscriptions() {
		return subscriptions;
	}

	public void shutdown() {
		if(consumer != null)
			consumer.shutdown();
	}
	
	public String getGroupId(){
		return groupId;
	}
	
}
