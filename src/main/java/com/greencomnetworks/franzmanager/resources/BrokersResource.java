package com.greencomnetworks.franzmanager.resources;

import com.greencomnetworks.franzmanager.entities.Broker;
import com.greencomnetworks.franzmanager.entities.Cluster;
import com.greencomnetworks.franzmanager.services.AdminClientService;
import com.greencomnetworks.franzmanager.services.BrokersService;
import com.greencomnetworks.franzmanager.services.ConstantsService;
import com.greencomnetworks.franzmanager.services.KafkaMetricsService;
import com.greencomnetworks.franzmanager.utils.FUtils;
import com.greencomnetworks.franzmanager.utils.KafkaUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Path("/brokers")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class BrokersResource {
    private static final Logger logger = LoggerFactory.getLogger(BrokersResource.class);

    private String clusterId;
    private Cluster cluster;
    private AdminClient adminClient;
    private HashMap<String, JMXConnector> jmxConnector;

    public BrokersResource(@HeaderParam("clusterId") String clusterId) {
        if (StringUtils.isEmpty(clusterId)) clusterId = "Default";
        this.clusterId = clusterId;
        this.adminClient = AdminClientService.getAdminClient(clusterId);
        this.jmxConnector = KafkaMetricsService.getJmxConnector(clusterId);
        for (Cluster cluster : ConstantsService.clusters) {
            if (StringUtils.equals(cluster.name, clusterId)) {
                this.cluster = cluster;
                break;
            }
        }
        if (this.cluster == null) {
            throw new NotFoundException("Cluster not found for id " + clusterId);
        }
    }

    /*
        @GET
        public List<Broker> getBrokers() {
            try {
                String hostName = InetAddress.getLocalHost().getHostName();
                Cluster cluster = ConstantsService.clusters.stream().filter(c -> c.name.equals(clusterId)).findAny().orElse(null);
                Collection<Node> brokers;
                try {
                    brokers = adminClient.describeCluster(new DescribeClusterOptions().timeoutMs(2000)).nodes().get().stream().map(broker -> {
                        if (broker.host().equals(hostName)) {
                            return new Node(broker.id(), "127.0.0.1", broker.port(), broker.rack());
                        }
                        return broker;
                    }).collect(Collectors.toList());
                } catch (Exception e) {
                    return Arrays.stream(this.cluster.brokersConnectString.split(",")).map(connectString -> {
                        return new Broker("?", connectString.split(":")[0], Integer.parseInt(connectString.split(":")[1]), null, (float) 0, (float) 0, Broker.State.UNSTABLE);
                    }).collect(Collectors.toList());
                }
                Collection<ConfigResource> configResources = brokers.stream().map(broker -> new ConfigResource(ConfigResource.Type.BROKER, broker.idString())).collect(Collectors.toSet());
                Map<ConfigResource, Config> brokersConfigs = adminClient.describeConfigs(configResources).all().get();

                List<Broker> brokerList = FUtils.getOrElse(brokers.stream().map(broker -> {
                    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, broker.idString());
                    Config config = brokersConfigs.get(configResource);

                    Map<String, String> configs = new HashMap<>();
                    for (ConfigEntry entry : config.entries()) {
                        configs.put(entry.name(), entry.value());
                    }

                    Float bytesIn = null;
                    Float bytesOut = null;
                    Broker.State brokerState = Broker.State.OK;
                    try {
                        MBeanServerConnection mbsc = jmxConnector.get(broker.host()).getMBeanServerConnection();
                        logger.info("WTF :: " + broker.host());
                        logger.info("WTF :: " + mbsc.toString());
                        bytesIn = Float.valueOf(mbsc.getAttribute(new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec"), "OneMinuteRate").toString());
                        bytesOut = Float.valueOf(mbsc.getAttribute(new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec"), "OneMinuteRate").toString());
                    } catch (Exception e) {
                        logger.error("Error while retrieving JMX data: {}", e.getMessage(), e);
                        brokerState = Broker.State.BROKEN;
                    }
                    return new Broker(broker.idString(), broker.host(), broker.port(), configs, bytesIn, bytesOut, brokerState);
                }).collect(Collectors.toList()), new ArrayList<>());

                Collection<Node> finalBrokers = brokers;
                Arrays.stream(cluster.brokersConnectString.split(",")).forEach(brokerString -> {
                    Node existingNode = finalBrokers.stream().filter(b -> b.host().equals(brokerString.split(":")[0])).findAny().orElse(null);
                    if (existingNode == null) {
                        brokerList.add(new Broker("?", brokerString.split(":")[0], Integer.parseInt(brokerString.split(":")[1]), null, (float) 0, (float) 0, Broker.State.BROKEN));
                    }
                });

                return brokerList;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException | UnknownHostException e) {
                throw new RuntimeException(e.getCause());
            }
        }
    */
    @GET
    public List<Broker> getBrokers(@QueryParam("withConfiguration") boolean withConfiguration) {
        logger.info("With configuration " + withConfiguration);

        List<Broker> knownBrokers = BrokersService.getKnownKafkaBrokers(clusterId);

        if (withConfiguration) {
            knownBrokers.forEach(broker -> { // if broker is okay, admin client should work.
                Map<String, String> configs;
                if (broker.state.equals(Broker.State.OK)) {
                    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, broker.id);
                    List<ConfigResource> configResources = FUtils.List.of(configResource);
                    try {
                        AdminClient adminClient = AdminClientService.connectToOneBroker(broker.host + ':' + broker.port);
                        Map<ConfigResource, Config> describeConfigsResult = adminClient.describeConfigs(configResources, new DescribeConfigsOptions().timeoutMs(3000)).all().get();
                        Config config = describeConfigsResult.get(configResource);
                        configs = new HashMap<>();
                        for (ConfigEntry entry : config.entries()) {
                            configs.put(entry.name(), entry.value());
                        }
                        adminClient.close();
                    } catch (Exception e) {
                        broker.state = Broker.State.BROKEN;
                        configs = null;
                    }
                } else {
                    configs = null;
                }
                if (configs == null) {
                    configs = new HashMap<>();
                    String zkString = ConstantsService.getCluster(clusterId).zookeeperConnectString;
                    configs.put("zookeeper.connect", zkString);
                }
                broker.configurations = configs;
            });
        }

        return knownBrokers;
    }

    @GET
    @Path("/{brokerId}")
    public Broker getBroker(@PathParam("brokerId") String brokerId) {
        try {
            Config config = KafkaUtils.describeBrokerConfig(adminClient, brokerId);
            if (config == null) {
                throw new NotFoundException("This broker (" + brokerId + ") doesn't exist.");
            }

            Collection<Node> brokers = adminClient.describeCluster().nodes().get();
            Node node = brokers.stream().filter(n -> n.idString().equals(brokerId)).findAny().orElse(null);
            if (node == null) {
                throw new NotFoundException("This broker (" + brokerId + ") doesn't exist.");
            }


            Map<String, String> configs = new HashMap<>();
            for (ConfigEntry entry : config.entries()) {
                configs.put(entry.name(), entry.value());
            }

            try {
                MBeanServerConnection mbsc = jmxConnector.get(node.host()).getMBeanServerConnection();
                Float bytesIn = Float.valueOf(mbsc.getAttribute(new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec"), "OneMinuteRate").toString());
                Float bytesOut = Float.valueOf(mbsc.getAttribute(new ObjectName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec"), "OneMinuteRate").toString());

                return new Broker(node.idString(), node.host(), node.port(), -1, configs, Broker.State.OK);
            } catch (Exception e) {
                logger.info("ERRORRRR: " + e.getMessage());
                return null;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }
}
