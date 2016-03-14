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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.moquette.BrokerConstants;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import io.moquette.server.kafka.KafkaService;
import io.moquette.spi.security.IAuthenticator;
import io.moquette.spi.security.IAuthorizator;

/**
 *
 * @author luca <luca.capra@create-net.org> 
 */
public class ConfigurationClassLoaderTest implements IAuthenticator, IAuthorizator {
    private static KafkaService kafka;
	Server m_server;
    IConfig m_config;
    
    protected void startServer(Properties props) throws IOException {
        m_server = new Server();
        m_config = new MemoryConfig(props);
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

    @After
    public void tearDown() throws Exception {
        m_server.stopServer();
        File dbFile = new File(m_config.getProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME));
        if (dbFile.exists()) {
            dbFile.delete();
        }
    }
    
    @Test
    public void loadAuthenticator() throws Exception {
        Properties props = new Properties(IntegrationUtils.prepareTestPropeties());
        props.setProperty("kafka_properties_file", "config/kafka.properties");
        props.setProperty(BrokerConstants.AUTHENTICATOR_CLASS_NAME, "io.moquette.server.ConfigurationClassLoaderTest");
        startServer(props);
        assertTrue(true);
        m_server.stopServer();
    }
    
    @Test
    public void loadAuthorizator() throws Exception {
        Properties props = new Properties(IntegrationUtils.prepareTestPropeties());
        props.setProperty("kafka_properties_file", "config/kafka.properties");
        props.setProperty(BrokerConstants.AUTHORIZATOR_CLASS_NAME, "io.moquette.server.ConfigurationClassLoaderTest");
        startServer(props);
        assertTrue(true);
        m_server.stopServer();
    }

    @Override
    public boolean checkValid(String username, byte[] password) {
        return true;
    }

    @Override
    public boolean canWrite(String topic, String user, String client) {
        return true;
    }

    @Override
    public boolean canRead(String topic, String user, String client) {
        return true;
    }
    
}
