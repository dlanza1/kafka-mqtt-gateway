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

import io.moquette.parser.proto.messages.ConnectMessage;
import io.moquette.parser.proto.messages.AbstractMessage;
import io.moquette.parser.proto.messages.AbstractMessage.QOSType;
import io.moquette.parser.proto.messages.ConnAckMessage;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.moquette.server.kafka.KafkaService;
import io.moquette.testclient.Client;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 *
 * @author andrea
 */
public class ServerLowlevelMessagesIntegrationTests {
    private static final Logger LOG = LoggerFactory.getLogger(ServerLowlevelMessagesIntegrationTests.class);
    static MqttClientPersistence s_dataStore;
	private static KafkaService kafka;
    Server m_server;
    Client m_client;
    IMqttClient m_willSubscriber;
    MessageCollector m_messageCollector;
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
        m_client = new Client("localhost");
        m_willSubscriber = new MqttClient("tcp://localhost:1883", "Subscriber", s_dataStore);
        m_messageCollector = new MessageCollector();
        m_willSubscriber.setCallback(m_messageCollector);
    }

    @After
    public void tearDown() throws Exception {
        m_client.close();
        LOG.debug("After raw client close");
        Thread.sleep(300); //to let the close event pass before server stop event
        m_server.stopServer();
        LOG.debug("After asked server to stop");
        IntegrationUtils.cleanPersistenceFile(m_config);
    }
    
    @Test
    public void elapseKeepAliveTime() throws InterruptedException {
        int keepAlive = 2; //secs
        ConnectMessage connectMessage = new ConnectMessage();
        connectMessage.setProtocolVersion((byte) 3);
        connectMessage.setClientID("FAKECLNT");
        connectMessage.setKeepAlive(keepAlive);
        m_client.sendMessage(connectMessage);
        
        //wait 2 times the keepAlive
        Thread.sleep(keepAlive * 2 * 1000);
        
        assertTrue(m_client.isConnectionLost());
    }
    
    @Test
    public void checkWillMessageIsWiredOnClientKeepAliveExpiry() throws Exception {
        LOG.info("*** checkWillMessageIsWiredOnClientKeepAliveExpiry ***");
        String willTestamentTopic = "/will/test";
        String willTestamentMsg = "Bye bye";
        
        m_willSubscriber.connect();
        m_willSubscriber.subscribe(willTestamentTopic, 0);
        
        int keepAlive = 2; //secs
        ConnectMessage connectMessage = new ConnectMessage();
        connectMessage.setProtocolVersion((byte) 3);
        connectMessage.setClientID("FAKECLNT");
        connectMessage.setKeepAlive(keepAlive);
        connectMessage.setWillFlag(true);
        connectMessage.setWillMessage(willTestamentMsg.getBytes());
        connectMessage.setWillTopic(willTestamentTopic);
        connectMessage.setWillQos(QOSType.MOST_ONE.byteValue());
        
        //Execute
        m_client.sendMessage(connectMessage);
        long connectTime = System.currentTimeMillis();

        //but after the 2 KEEP ALIVE timeout expires it gets fired,
        //NB it's 1,5 * KEEP_ALIVE so 3 secs and some millis to propagate the message
        MqttMessage msg = m_messageCollector.getMessage(3300);
        long willMessageReceiveTime = System.currentTimeMillis();
        if (msg == null) {
            LOG.warn("testament message is null");
        }
        assertNotNull("the will message should be fired after keep alive!", msg);
        //the will message hasn't to be received before the elapsing of Keep Alive timeout
        assertTrue(willMessageReceiveTime - connectTime  > 3000);
        
        assertEquals(willTestamentMsg, new String(msg.getPayload()));
        m_willSubscriber.disconnect();
    }
    
    AbstractMessage receivedMsg;
    
    @Test
    public void checkRejectConnectWithEmptyClientID() throws InterruptedException {
        LOG.info("*** checkRejectConnectWithEmptyClientID ***");
        final CountDownLatch latch = new CountDownLatch(1);
        m_client.setCallback(new Client.ICallback() {

            public void call(AbstractMessage msg) {
                receivedMsg = msg;
                latch.countDown();
            }
        });
        
        int keepAlive = 2; //secs
        ConnectMessage connectMessage = new ConnectMessage();
        connectMessage.setProtocolVersion((byte) 4);
        connectMessage.setClientID("");
        connectMessage.setKeepAlive(keepAlive);
        connectMessage.setWillFlag(false);
        connectMessage.setWillQos(QOSType.MOST_ONE.byteValue());
        
        //Execute
        m_client.sendMessage(connectMessage);
        
        latch.await(200, TimeUnit.MILLISECONDS);
        
        assertTrue(receivedMsg instanceof ConnAckMessage);
        ConnAckMessage connAck = (ConnAckMessage) receivedMsg;
        assertEquals(ConnAckMessage.IDENTIFIER_REJECTED, connAck.getReturnCode());
    }
    
}
