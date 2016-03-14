package io.moquette.server.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class KafkaService {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);

	private KafkaServerStartable kafka;
	private TestingServer zkTestServer;

	public KafkaService() throws Exception {
		zkTestServer = new TestingServer(2181, new File("zookeeper-data"));
        KafkaConfig kafkaConfig = new KafkaConfig(createProperties("kafkalog", 9092, 1, "localhost:2181"), false);
        kafka = new KafkaServerStartable(kafkaConfig);
        
        LOG.info("Kafka sercice running...");
	}
	
	public KafkaService start() {
		kafka.startup();
		
//      String zookeeperConnect = "localhost:2181";
//		String topic = ".topic";
//		
//		ZkClient zkClient = new ZkClient(
//              zookeeperConnect,
//              10 * 1000,
//              8 * 1000,
//              ZKStringSerializer$.MODULE$);
//		ZkUtils zhUtil = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false);
//		AdminUtils.createTopic(zhUtil, topic, 1, 1, new Properties());
      
		
		return this;
	}

	public void shutdown() throws IOException {
		kafka.shutdown();
    	zkTestServer.stop();
    	
    	deleteDir(new File("zookeeper-data"));
    	deleteDir(new File("kafkalog"));
    	
    	LOG.info("Kafka sercice stopped");
	}
	
    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        return dir.delete();
    }
    
    private static Properties createProperties(String logDir, int port, int brokerId, String zookeeper) {
    	Properties properties = new Properties();
    	properties.put("port", port+"");
    	properties.put("brokerid", brokerId+"");
    	properties.put("log.dir", logDir);
    	properties.put("zookeeper.connect", zookeeper);
    	return properties;
    }
	
}
