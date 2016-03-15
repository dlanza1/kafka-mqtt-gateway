package io.moquette.spi.impl.kafka;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.spi.impl.ProtocolProcessor;
import io.moquette.spi.impl.subscriptions.Subscription;

public class ConsumerGroup {
	
	private static final Logger LOG = LoggerFactory.getLogger(TopicCounsumerGroup.class);
	
	private HashMap<String, TopicCounsumerGroup> threadGroups;
	
	private ProtocolProcessor protocolProcessor;

	private Properties props;

	public ConsumerGroup(Properties props, ProtocolProcessor protocolProcessor) {		
		threadGroups = new HashMap<String, TopicCounsumerGroup>();
		
		this.protocolProcessor = protocolProcessor;
		this.props = props;
		
		if(!props.containsKey("group.id"))
			props.put("group.id",  new BigInteger(130, new SecureRandom()).toString(32));
	}

	public synchronized void shutdown() {
		System.out.println("Shutting down...");
		
		for (Iterator<TopicCounsumerGroup> iterator = threadGroups.values().iterator(); iterator.hasNext();) {
			TopicCounsumerGroup topicCounsumerGroup = iterator.next();
			
			topicCounsumerGroup.shutdown();
		}
	}

	public synchronized void subscribe(Subscription newSubscription, int numThreads) {
		LOG.debug("Subscribing: " + newSubscription + " (" + numThreads + ")");
		
		String topicFilter = newSubscription.getTopicFilter();
		
		TopicCounsumerGroup topicConsumerGroup;
		if(threadGroups.containsKey(topicFilter)){
			topicConsumerGroup = threadGroups.get(topicFilter);

			topicConsumerGroup.subscribe(newSubscription);
		}else{
			topicConsumerGroup = new TopicCounsumerGroup(
					protocolProcessor,
					props, 
					topicFilter);
			
			topicConsumerGroup.subscribe(newSubscription);
			topicConsumerGroup.init(1);
						
			threadGroups.put(topicFilter, topicConsumerGroup);
		}
	}
	
	public synchronized void unsubscribe(String topic, String clientID) {
		if(threadGroups.containsKey(topic)){
			TopicCounsumerGroup threadGroup = threadGroups.get(topic);
		
			threadGroup.unsubscribe(clientID);
			
			if(threadGroup.getSubscriptions().size() <= 0){
				threadGroup.shutdown();
				threadGroups.remove(topic);
				
				LOG.debug("TopicConsumerGroup: " + topic + " => Removed");
			}
		}
	}

}