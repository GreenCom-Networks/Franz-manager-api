package com.greencomnetworks.franzmanager.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.greencomnetworks.franzmanager.entities.Cluster;
import com.greencomnetworks.franzmanager.utils.CustomObjectMapper;
import com.greencomnetworks.franzmanager.utils.FUtils;
import com.greencomnetworks.franzmanager.utils.configs.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConstantsService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMetricsService.class);
    public static List<Cluster> clusters = new ArrayList<>();

    public static void init() throws RuntimeException {
        logger.info("Checking constants.");
        String KAFKA_CONF = System.getenv("KAFKA_CONF");

        if (KAFKA_CONF == null) {
            throw new ConfigException("Missing environment variable KAFKA_CONF.");
        }

        try {
            clusters = CustomObjectMapper.defaultInstance().readValue(KAFKA_CONF, new TypeReference<List<Cluster>>() {
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        logger.info("---------- <CLUSTERS> ----------");
        clusters.forEach(cluster -> logger.info(cluster.toString()));
        logger.info("---------- </CLUSTERS> ----------");
    }

    public static Cluster getCluster(String clusterId){
        return clusters.stream().filter(c -> c.name.equals(clusterId)).findFirst().get();
    }
}
