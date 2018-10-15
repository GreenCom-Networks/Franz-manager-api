package com.greencomnetworks.franzmanager.services;

import com.greencomnetworks.franzmanager.entities.Metric;
import com.greencomnetworks.franzmanager.utils.FUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.Node;

import javax.management.*;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TopicMetricsService {
    private static HashMap<String, HashMap<String, HashMap<String, Metric>>> topicMetrics = new HashMap<>();

    public static void init() {
        topicMetrics = new HashMap<>();
        new Thread(new CheckMetrics(), "CheckMetrics").start();
    }

    public static HashMap<String, HashMap<String, Metric>> getTopicsMetrics(String clusterId) {
        if (clusterId == null) {
            return null;
        }

        return topicMetrics.get(clusterId);
    }

    private static class CheckMetrics implements Runnable {
        public void run() {
            while (true) {
                try {
                    Thread.sleep(15000); // wait 15 sc before first try.

                    HashMap<String, HashMap<String, JMXConnector>> jmxConnector = KafkaMetricsService.getJmxConnectors();

                    for (String clusterId : jmxConnector.keySet()) { // for each clusters;
                        HashMap<String, JMXConnector> clusterMBeanServerConnection = jmxConnector.get(clusterId);
                        AdminClient adminClient = AdminClientService.getAdminClient(clusterId);
                        ListTopicsOptions listTopicsOptions = new ListTopicsOptions().listInternal(true);
                        Set<String> topics = adminClient.listTopics(listTopicsOptions).names().get();

                        Collection<Node> brokers = adminClient.describeCluster().nodes().get().stream().map(broker -> {
                            try {
                                if (broker.host().equals(InetAddress.getLocalHost().getHostName())) {
                                    return new Node(broker.id(), "127.0.0.1", broker.port(), broker.rack());
                                }
                            } catch (UnknownHostException e) {
                                throw new RuntimeException(e);
                            }
                            return broker;
                        }).collect(Collectors.toList());

                        HashMap<String, HashMap<String, Metric>> clusterTopicsMetrics = new HashMap<>();

                        topics.forEach(topic -> {
                            HashMap<String, Metric> brokerTopicMetrics = new HashMap<>();

                            for (String brokerHost : clusterMBeanServerConnection.keySet()) { // for each brokers.
                                try {
                                    MBeanServerConnection mbsc = clusterMBeanServerConnection.get(brokerHost).getMBeanServerConnection();
                                    Node currentBroker = brokers.stream().filter(n -> n.host().equals(brokerHost)).findFirst().get();
                                    String queryString = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=" + topic;
                                    String metricName = "MessagesInPerSec";
                                    Metric metric = new Metric("BrokerTopicMetrics", metricName, currentBroker.id(), new HashMap<>());
                                    ObjectName objName = new ObjectName(queryString);
                                    MBeanInfo beanInfo = mbsc.getMBeanInfo(objName);
                                    for (MBeanAttributeInfo attr : beanInfo.getAttributes()) {
                                        Object value = mbsc.getAttribute(objName, attr.getName());
                                        if (NumberUtils.isCreatable(String.valueOf(value))) {
                                            Float floatValue = Float.parseFloat(String.valueOf(value));
                                            Float existingValue = Float.parseFloat(String.valueOf(FUtils.getOrElse(metric.metrics.get(attr.getName()), 0)));
                                            metric.metrics.put(attr.getName(), floatValue + existingValue);
                                        } else {
                                            metric.metrics.put(attr.getName(), value);
                                        }
                                    }
                                    brokerTopicMetrics.put(currentBroker.idString(), metric);
                                } catch (InstanceNotFoundException | MalformedObjectNameException | AttributeNotFoundException e) {
                                    // we don't care
                                } catch (IOException | ReflectionException | IntrospectionException | MBeanException e) {
                                    e.printStackTrace();
                                }
                            }
                            clusterTopicsMetrics.put(topic, brokerTopicMetrics);

                        });

                        topicMetrics.put(clusterId, clusterTopicsMetrics);
                    }
                    Thread.sleep(900000); // every 15 minutes
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
