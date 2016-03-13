package io.moquette.spi.impl.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.parser.proto.messages.AbstractMessage.QOSType;
import io.moquette.spi.impl.ProtocolProcessor;
import io.moquette.spi.impl.subscriptions.Subscription;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;

public class ConsumerGroup {
	
	private static final Logger LOG = LoggerFactory.getLogger(TopicCounsumerGroup.class);
	
	private final ConsumerConnector consumer;
	private HashMap<String, TopicCounsumerGroup> threadGroups;
	private ProtocolProcessor protocolProcessor;

	public ConsumerGroup(String a_zookeeper, String a_groupId, ProtocolProcessor protocolProcessor) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
		
		//Defaults
//		props.put("zookeeper.session.timeout.ms", "400");
//		props.put("zookeeper.sync.time.ms", "200");
//		props.put("auto.commit.interval.ms", "1000");
		
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		
		threadGroups = new HashMap<String, TopicCounsumerGroup>();
		
		this.protocolProcessor = protocolProcessor;
	}

	public void shutdown() {
		System.out.println("Shutting down...");
		
		if (consumer != null)
			consumer.shutdown();
	}

	public void subscribe(Subscription newSubscription, int numThreads) {
		String topicFilter = newSubscription.getTopicFilter();
		
		TopicCounsumerGroup threadGroup;
		if(threadGroups.containsKey(topicFilter)){
			threadGroup = threadGroups.get(topicFilter);
			
			threadGroup.subscribe(newSubscription);
		}else{
			threadGroup = new TopicCounsumerGroup(topicFilter);

			List<KafkaStream<byte[], byte[]>> streams = consumer.createMessageStreamsByFilter(
					new Whitelist(topicFilter), 
					numThreads);
			
			int threadIdx = 0;
			for (final KafkaStream<byte[], byte[]> stream : streams) {
				TopicConsumer consumer = new TopicConsumer(protocolProcessor, 
						stream, 
						threadGroup,
						threadIdx++);
				
				threadGroup.subscribe(newSubscription);
				consumer.start();
			}
			
			threadGroups.put(topicFilter, threadGroup);
		}
	}
	
	public void unsubscribe(String topic, String clientID) {
		if(threadGroups.containsKey(topic)){
			TopicCounsumerGroup threadGroup = threadGroups.get(topic);
		
			threadGroup.unsubscribe(clientID);
			
			if(threadGroup.getSubscriptions().size() <= 0){
				threadGroup.interrupt();
				threadGroups.remove(topic);
				
				LOG.debug("TopicConsumerGroup: " + topic + " => Removed");
			}
		}
	}

	public static void main(String[] args) {
		String zooKeeper = "nodo1:2181,nodo4:2181,nodo6:2181";
		String groupId = "any";
		String topic = "page_visits___google";
		int threads = 1;

		final ConsumerGroup example = new ConsumerGroup(zooKeeper, groupId, null);
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
	        public void run() {
	        	example.shutdown();
	        }
	    }, "Shutdown-thread"));
		
		example.subscribe(new Subscription(null, topic, QOSType.LEAST_ONE), threads);
	}
	
}