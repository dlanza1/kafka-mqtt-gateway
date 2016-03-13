package io.moquette.spi.impl.kafka;

import io.moquette.spi.impl.subscriptions.SubscriptionsStore;
import kafka.common.Topic;

public class TopicFilter extends kafka.consumer.TopicFilter {

	String topicFIlter;
	
	public TopicFilter(String topicFilter) {
		super(topicFilter.replace("/", "_bs_"));
		this.topicFIlter = topicFilter;
	}

	@Override
	public boolean isTopicAllowed(String topic, boolean excludeInternalTopics) {
		boolean match = SubscriptionsStore.matchTopics(topic.replace("_bs_", "/"), topicFIlter);
		
		return match && !(Topic.InternalTopics().contains(topic) && excludeInternalTopics); 
	}

}
