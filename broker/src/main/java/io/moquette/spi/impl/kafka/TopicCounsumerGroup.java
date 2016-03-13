package io.moquette.spi.impl.kafka;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.spi.impl.subscriptions.Subscription;

public class TopicCounsumerGroup extends ThreadGroup{
	
	private static final Logger LOG = LoggerFactory.getLogger(TopicCounsumerGroup.class);

	private HashSet<Subscription> subscriptions = new HashSet<Subscription>();
	
	public TopicCounsumerGroup(String topicFilter) {
		super(topicFilter);
		
		LOG.debug("New TopicConsumerGroup: " + getName());
	}

	public synchronized void subscribe(Subscription newSubscription) {
		LOG.debug("TopicConsumerGroup: " + getName() + " => New Subscription: " + newSubscription);
		
		subscriptions.add(newSubscription);
	}

	public synchronized void unsubscribe(String clientIDToRemove) {
		LOG.debug("TopicConsumerGroup: " + getName() + " => Removing Subscriptions for: " + clientIDToRemove);
		
		Iterator<Subscription> it = subscriptions.iterator();
		while (it.hasNext()) {
			Subscription subscription = (Subscription) it.next();
			
			if(subscription.getClientId().equals(clientIDToRemove)){
				it.remove();
				
				LOG.debug("TopicConsumerGroup: " + getName() + " => Removed Subscription: " + subscription);
			}
		}
	}

	public synchronized Set<Subscription> getSubscriptions() {
		return subscriptions;
	}
	
}
