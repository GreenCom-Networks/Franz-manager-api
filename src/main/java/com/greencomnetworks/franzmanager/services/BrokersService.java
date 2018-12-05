package com.greencomnetworks.franzmanager.services;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.greencomnetworks.franzmanager.entities.Broker;
import com.greencomnetworks.franzmanager.resources.BrokersResource;
import com.greencomnetworks.franzmanager.utils.CustomObjectMapper;
import com.greencomnetworks.franzmanager.utils.FUtils;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class BrokersService {
    private static final Logger logger = LoggerFactory.getLogger(BrokersResource.class);
    private static Map<String, ArrayList<Broker>> clustersKnownKafkaBrokers = new HashMap<>();

    public static void init() {
        logger.info("Starting DiscoverKafkaBrokers loop");
        new Thread(new DiscoverKafkaBrokers(), "DiscoverKafkaBrokers").start();
    }

    public static ArrayList<Broker> getKnownKafkaBrokers(String clusterId) {
        return clustersKnownKafkaBrokers.getOrDefault(clusterId, new ArrayList<>());
    }

    private static class DiscoverKafkaBrokers implements Runnable {
        public void run() {
            try {
                while (true) {
                    ConstantsService.clusters.forEach(cluster -> {
                        ArrayList<Broker> knownKafkaBrokers = clustersKnownKafkaBrokers.computeIfAbsent(cluster.name, n -> new ArrayList<>());
                        ArrayList<Broker> currentKafkaBrokers = this.getCurrentKafkaBrokers(cluster.name);

                        // check unknown brokers
                        currentKafkaBrokers.forEach(broker -> {
                            if (FUtils.findInCollection(knownKafkaBrokers, b -> b.equals(broker)) == null) {
                                knownKafkaBrokers.add(broker);
                            }
                        });

                        // check disapeared brokers
                        knownKafkaBrokers.forEach(broker -> {
                            Broker currentKafkaBroker = FUtils.findInCollection(currentKafkaBrokers, b -> b.equals(broker));
                            if (currentKafkaBroker == null) {
                                broker.state = Broker.State.BROKEN;
                            } else {
                                broker.state = Broker.State.OK;
                                broker.id = currentKafkaBroker.id;
                            }
                        });

                        // add broker unfindable but that should exist
                        Arrays.stream(cluster.brokersConnectString.split(",")).forEach(brokerString -> {
                            String brokerHost = brokerString.split(":")[0];
                            int brokerPort = Integer.parseInt(brokerString.split(":")[1]);
                            String brokerJmxConnectString = Arrays.stream(cluster.jmxConnectString.split(",")).filter(str -> {
                                return str.toString().split(":")[0].equals(brokerHost);
                            }).findFirst().orElse(null);
                            int brokerJmxPort = -1;
                            if (brokerJmxConnectString != null) {
                                brokerJmxPort = Integer.parseInt(brokerJmxConnectString.split(":")[1]);
                            }

                            if (FUtils.findInCollection(currentKafkaBrokers, b -> b.port.equals(brokerPort) && b.host.equals(brokerHost)) == null
                                    && FUtils.findInCollection(knownKafkaBrokers, b -> b.port.equals(brokerPort) && b.host.equals(brokerHost)) == null) {
                                knownKafkaBrokers.add(new Broker("?", brokerHost, brokerPort, brokerJmxPort, null, Broker.State.BROKEN));
                            }
                        });
                        clustersKnownKafkaBrokers.put(cluster.name, knownKafkaBrokers);
                    });
                    Thread.sleep(10000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private ArrayList<Broker> getCurrentKafkaBrokers(String clusterId) {
            ArrayList<Broker> result = new ArrayList<>();
            try {
                String hostName = InetAddress.getLocalHost().getHostName();
                ZooKeeper zooKeeper = ZookeeperService.getZookeeperConnection(clusterId);
                if (zooKeeper == null) {
                    throw new RuntimeException("The cluster " + clusterId + " does not exist.");
                }

                List<String> children = zooKeeper.getChildren("/brokers/ids", false);
                children.forEach(child -> {
                    try {
                        byte[] zkData = zooKeeper.getData("/brokers/ids/" + child, false, null);
                        String zkDataString = new String(zkData, StandardCharsets.UTF_8);
                        ZkKafkaBroker zkKafkaBroker = new CustomObjectMapper().readValue(zkDataString, ZkKafkaBroker.class);
                        String host = zkKafkaBroker.host.equals(hostName) ? "127.0.0.1" : zkKafkaBroker.host.split(":")[0];
                        result.add(new Broker(child, host, zkKafkaBroker.port, zkKafkaBroker.jmxPort, null, Broker.State.OK));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                logger.error(e.toString());
                e.printStackTrace();
            }
            return result;
        }
    }

    private static class ZkKafkaBroker {
        public String host;
        public long timestamp;
        public Integer port;
        public Integer jmxPort;

        @JsonCreator
        public ZkKafkaBroker(@JsonProperty("jmx_port") Integer jmxPort,
                             @JsonProperty("host") String host,
                             @JsonProperty("timestamp") long timestamp,
                             @JsonProperty("port") Integer port) {
            this.jmxPort = jmxPort;
            this.host = host;
            this.timestamp = timestamp;
            this.port = port;
        }

        public String toString() {
            return "ZkKafkaBroker " + host + "\n" + "port:" + port +
                    "\ntimestamp: " + timestamp + "\njmxPort: " + jmxPort;
        }
    }
}
