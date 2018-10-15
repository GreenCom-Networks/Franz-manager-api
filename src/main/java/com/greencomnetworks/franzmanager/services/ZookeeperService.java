package com.greencomnetworks.franzmanager.services;

import com.greencomnetworks.franzmanager.entities.Broker;
import com.greencomnetworks.franzmanager.resources.BrokersResource;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ZookeeperService {
    private static final Logger logger = LoggerFactory.getLogger(BrokersResource.class);
    private static Map<String, ZooKeeper> zookeeperConnections = new HashMap<>();

    public static void init() {
        ConstantsService.clusters.forEach(cluster -> {
            ZooKeeper zooKeeper;
            try {
                zooKeeper = new ZooKeeper(cluster.zookeeperConnectString, 5000, null);
            } catch (IOException e) {
                throw new RuntimeException("Cannot connect to zookeeper " + cluster.zookeeperConnectString);
            }
            zookeeperConnections.put(cluster.name, zooKeeper);
        });
    }

    public static ZooKeeper getZookeeperConnection(String clusterId) {
        return zookeeperConnections.getOrDefault(clusterId, null);
    }
}