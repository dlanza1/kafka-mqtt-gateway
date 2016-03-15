package io.moquette.spi.impl.kafka;

import org.junit.Test;

import junit.framework.Assert;

public class TopicCounsumerGroupTest {

	@Test
	public void toKafkaTopicFilter(){
		Assert.assertEquals(".*", TopicCounsumerGroup.toKafkaTopicFilter("#"));
		Assert.assertEquals("[^.]+.*", TopicCounsumerGroup.toKafkaTopicFilter("+/#"));
		Assert.assertEquals(".aaa.[^.]+.bbb.[^.]+.qqq.*", TopicCounsumerGroup.toKafkaTopicFilter("/aaa/+/bbb/+/qqq/#"));
	}
	
}
