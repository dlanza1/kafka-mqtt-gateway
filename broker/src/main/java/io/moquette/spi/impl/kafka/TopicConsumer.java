package io.moquette.spi.impl.kafka;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.parser.proto.messages.AbstractMessage.QOSType;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.impl.ProtocolProcessor;
import io.moquette.spi.impl.subscriptions.Subscription;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class TopicConsumer extends Thread {
	
	private static final Logger LOG = LoggerFactory.getLogger(TopicConsumer.class);
	
	private KafkaStream<byte[], byte[]> stream;

	private ProtocolProcessor protocolProcessor;

	private Set<Subscription> subscriptions;

	public TopicConsumer(ProtocolProcessor protocolProcessor, 
			KafkaStream<byte[], byte[]> stream, 
			TopicCounsumerGroup group,
			int threadNum) {
		super(group, group.getName() + "-" + threadNum);
		
		LOG.debug("New TopicConsumerThread: " + getName());
		
		this.stream = stream;
		this.protocolProcessor = protocolProcessor;
		this.subscriptions = group.getSubscriptions();
		
		setDaemon(true);
	}

	@Override
	public void run() {
		LOG.debug("TopicConsumerThread: " + getName() + " => Running ...");
		
		try{
			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				LOG.debug("TopicConsumerThread: " + getName() + " => New message: " + msgAndMetadata);
				
				StoredMessage pubMsg = new StoredMessage(msgAndMetadata.message(), QOSType.MOST_ONE, msgAndMetadata.topic());
				
				try{
					protocolProcessor.route2Subscribers(subscriptions, pubMsg);
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}catch(Exception e){
			LOG.debug("TopicConsumerThread: " + getName() + " => Interrupted");
		}
	}
	
}