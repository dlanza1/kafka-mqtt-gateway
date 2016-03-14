package io.moquette.spi.impl.kafka;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.impl.ProtocolProcessor;
import io.moquette.spi.impl.subscriptions.Subscription;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class TopicConsumer extends Thread {

	private static final Logger LOG = LoggerFactory.getLogger(TopicConsumer.class);

	private KafkaStream<byte[], byte[]> stream;

	private ProtocolProcessor protocolProcessor;

	private Set<Subscription> subscriptions;

	public TopicConsumer(ProtocolProcessor protocolProcessor, KafkaStream<byte[], byte[]> stream,
			Set<Subscription> subscriptions, String name) {
		super(name);

		LOG.debug("New TopicConsumer: " + getName());

		this.stream = stream;
		this.protocolProcessor = protocolProcessor;
		this.subscriptions = subscriptions;

		setDaemon(true);
	}

	@Override
	public void run() {
		LOG.debug("TopicConsumer: " + getName() + " => Running ...");

		try {
			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				LOG.debug("TopicConsumer: " + getName() + " => New message: " + msgAndMetadata);
				
				StoredMessage pubMsg = toStoredMessage(msgAndMetadata.message());

				try {
					protocolProcessor.route2Subscribers(subscriptions, pubMsg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			LOG.debug("TopicConsumer: " + getName() + " => Interrupted");
		}
	}

	private StoredMessage toStoredMessage(byte[] payload) {

		ByteArrayInputStream bis = new ByteArrayInputStream(payload);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);

			return (StoredMessage) in.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				bis.close();
			} catch (IOException ex) {
			}
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
			}
		}

		return null;
	}

}