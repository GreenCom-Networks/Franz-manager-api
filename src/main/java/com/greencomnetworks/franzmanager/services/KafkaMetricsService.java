package com.greencomnetworks.franzmanager.services;

import com.greencomnetworks.franzmanager.entities.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;

public class KafkaMetricsService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMetricsService.class);
    private static HashMap<String, HashMap<String, JMXConnector>> jmxConnector = new HashMap<>();

    public static void init() {
        logger.info("Starting jmx connectivity check loop");
        new Thread(new JmxConnectivityCheck(), "JmxConnectivityCheck").start();
    }

    public static HashMap<String, JMXConnector> getJmxConnector(String clusterId) {
        if (clusterId == null) {
            clusterId = "Default";
        }
        return jmxConnector.get(clusterId);
    }

    public static HashMap<String, HashMap<String, JMXConnector>> getJmxConnectors() {
        return jmxConnector;
    }

    private static class JmxConnectivityCheck implements Runnable {
        public void run() {
            while (true) {
                try {
                    ConstantsService.clusters.forEach(cluster -> {
                        jmxConnector.computeIfAbsent(cluster.name, k -> new HashMap<>());
                        for (String url : cluster.jmxConnectString.split(",")) {
                            HashMap<String, JMXConnector> mbscs = jmxConnector.get(cluster.name);
                            try {
                                mbscs.get(url).getMBeanServerConnection();
                            } catch (IOException | NullPointerException e) {
                                logger.info("Connecting to jmx -- url: " + url.split(":")[0] + " -- cluster: " + cluster.name);
                                connectJmx(cluster, url);
                            }
                        }
                    });
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private static void connectJmx(Cluster cluster, String url) {
            try {
                JMXServiceURL jmxUrl = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + url + "/jmxrmi");
                JMXConnector jmxc = JMXConnectorFactory.connect(jmxUrl, null);
                jmxConnector.get(cluster.name).put(url, jmxc);
                logger.info("__ connected " + url + " -- " + cluster.name);
            } catch (MalformedURLException e) {
                throw new RuntimeException("The following url has a bad format : " + url, e);
            } catch (IOException e) {
                logger.error("Cannot connect to the following url '{}': {}", url, e.getMessage());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    // don't care.
                }
            }
        }
    }
}