/*
 * Copyright (c) 2012-2015 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.moquette.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.moquette.server.kafka.KafkaService;

/**
 *
 * @author andrea
 */
public class ServerIntegrationFuseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ServerIntegrationPahoTest.class);

	private static KafkaService kafka;

    Server m_server;
    MQTT m_mqtt;
    BlockingConnection m_subscriber;
    BlockingConnection m_publisher;
    IConfig m_config;
    
    protected void startServer() throws IOException {
        m_server = new Server();
        final Properties configProps = IntegrationUtils.prepareTestPropeties();
        m_config = new MemoryConfig(configProps);
        m_server.startServer(m_config);
    }

    @BeforeClass
    public static void beforeTests() throws Exception {
        kafka = new KafkaService().start();
    }

    @AfterClass
    public static void afterTests() throws Exception {
    	kafka.shutdown();
    }
    
    @Before
    public void setUp() throws Exception {
        startServer();

        m_mqtt = new MQTT();
        m_mqtt.setHost("localhost", 1883);
    }

    @After
    public void tearDown() throws Exception {
        if (m_subscriber != null) {
            m_subscriber.disconnect();
        }
        
        if (m_publisher != null) {
            m_publisher.disconnect();
        }

        m_server.stopServer();
        IntegrationUtils.cleanPersistenceFile(m_config);
    }

    @Test
    public void checkWillTestamentIsPublishedOnConnectionKill_noRetain() throws Exception {
        LOG.info("checkWillTestamentIsPublishedOnConnectionKill");
        
        String willTestamentTopic = "/will/test";
        String willTestamentMsg = "Bye bye";
        
        MQTT mqtt = new MQTT();
        mqtt.setHost("localhost", 1883); 
        mqtt.setClientId("WillTestamentPublisher");
        mqtt.setWillRetain(false);
        mqtt.setWillMessage(willTestamentMsg);
        mqtt.setWillTopic(willTestamentTopic);
        m_publisher = mqtt.blockingConnection();
        m_publisher.connect();
        
        m_mqtt.setHost("localhost", 1883); 
        m_mqtt.setCleanSession(false);
        m_mqtt.setClientId("Subscriber");
        m_subscriber = m_mqtt.blockingConnection();
        m_subscriber.connect();
//        Topic[] topics = new Topic[]{new Topic(willTestamentTopic, QoS.AT_LEAST_ONCE)};
        Topic[] topics = new Topic[]{new Topic(willTestamentTopic, QoS.AT_MOST_ONCE)};
        m_subscriber.subscribe(topics);
        
        //Exercise, kill the publisher connection
        m_publisher.kill();
        
        Thread.sleep(500);

        //Verify, that the testament is fired
        Message msg = m_subscriber.receive(5, TimeUnit.SECONDS);
        assertNotNull("We should get notified with 'Will' message", msg);
        msg.ack();
        assertEquals(willTestamentMsg, new String(msg.getPayload()));
    }
}
