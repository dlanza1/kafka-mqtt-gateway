package io.moquette.spi.impl.kafka;

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
		
		TopicCounsumerGroup threadGroup;
		if(threadGroups.containsKey(topicFilter)){
			threadGroup = threadGroups.get(topicFilter);

			threadGroup.subscribe(newSubscription);
		}else{
			threadGroup = new TopicCounsumerGroup(
					protocolProcessor,
					props, 
					topicFilter);
			
			threadGroup.subscribe(newSubscription);
			threadGroup.ini(1);
						
			threadGroups.put(topicFilter, threadGroup);
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