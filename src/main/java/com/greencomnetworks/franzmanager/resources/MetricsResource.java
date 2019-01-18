package com.greencomnetworks.franzmanager.resources;

import com.greencomnetworks.franzmanager.entities.Broker;
import com.greencomnetworks.franzmanager.entities.Metric;
import com.greencomnetworks.franzmanager.services.AdminClientService;
import com.greencomnetworks.franzmanager.services.BrokersService;
import com.greencomnetworks.franzmanager.services.KafkaMetricsService;
import com.greencomnetworks.franzmanager.services.TopicMetricsService;
import com.greencomnetworks.franzmanager.utils.FUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Path("/metrics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MetricsResource {
    private static final Logger logger = LoggerFactory.getLogger(MetricsResource.class);

    private HashMap<String, JMXConnector> jmxConnectors;
    private AdminClient adminClient;
    private String clusterId;

    public MetricsResource(@HeaderParam("clusterId") String clusterId) {
        this.jmxConnectors = KafkaMetricsService.getJmxConnector(clusterId);
        this.adminClient = AdminClientService.getAdminClient(clusterId);
        this.clusterId = clusterId;
    }

    @GET
    public List<Metric> get(@QueryParam("metricLocation") String metricLocation,
                            @QueryParam("metricType") String metricType,
                            @QueryParam("metricName") String metricName,
                            @QueryParam("additional") String additional) throws IOException, AttributeNotFoundException, MBeanException, ReflectionException, MalformedObjectNameException, ExecutionException, InterruptedException {
        if (StringUtils.isEmpty(metricLocation)) {
            throw new BadRequestException("Missing query parameter 'metricLocation'");
        } else if (StringUtils.isEmpty(metricType)) {
            throw new BadRequestException("Missing query parameter 'metricType'");
        }

        String queryString = metricLocation + ":";
        queryString += "type=" + metricType;
        if (metricName != null && !metricName.equals("HeapMemoryUsage")) {
            queryString += ",name=" + metricName;
        }

        if (additional != null) {
            queryString += "," + additional;
        }

        ObjectName objName = new ObjectName(queryString);
        ArrayList<Broker> knownKafkaBrokers = BrokersService.getKnownKafkaBrokers(clusterId);
        List<Metric> metrics = new ArrayList<>();
        for (String brokerHost : jmxConnectors.keySet()) {
            try {
                MBeanServerConnection mbsc = jmxConnectors.get(brokerHost).getMBeanServerConnection();
                String host = brokerHost.split(":")[0];
                Integer port = Integer.parseInt(brokerHost.split(":")[1]);
                logger.warn(host + " , " + port);
                Broker currentBroker = FUtils.findInCollection(knownKafkaBrokers, b -> b.jmxPort.equals(port) && b.host.equals(host));
                Metric metric = new Metric(metricType, metricName, Integer.parseInt(currentBroker.id), new HashMap<>());
                MBeanInfo beanInfo = mbsc.getMBeanInfo(objName);
                for (MBeanAttributeInfo attr : beanInfo.getAttributes()) {
                    if (metricName != null && metricName.equals("HeapMemoryUsage")) { //specific case for this metric
                        CompositeData cd = (CompositeData) mbsc.getAttribute(objName, metricName);
                        Arrays.stream(new String[]{"committed", "init", "max", "used"}).forEach(key -> {
                            metric.metrics.put(key, cd.get(key));
                        });
                        logger.warn(cd.values().toString());
                    } else {
                        Object value = mbsc.getAttribute(objName, attr.getName());
                        if (NumberUtils.isCreatable(String.valueOf(value))) {
                            Float floatValue = Float.parseFloat(String.valueOf(value));
                            Float existingValue = Float.parseFloat(String.valueOf(FUtils.getOrElse(metric.metrics.get(attr.getName()), 0)));
                            metric.metrics.put(attr.getName(), floatValue + existingValue);
                        } else {
                            metric.metrics.put(attr.getName(), value);
                        }
                    }
                }
                metrics.add(metric);
            } catch (IntrospectionException e) {
                // that means a jmx server is not available
                logger.warn("A jmx server cannot be reached : {}", e.getMessage());
            } catch (InstanceNotFoundException | NoSuchElementException | NullPointerException e) {
                logger.warn("Cannot retrieved this metric {{}}.", queryString);
            } catch (IOException e) {
                logger.warn("A jmx connection is broken.", queryString);
            }
        }
        return metrics;
    }

    @Path("/topics")
    @GET
    public HashMap<String, HashMap<String, Metric>> getTopicsMetric() {
        return TopicMetricsService.getTopicsMetrics(clusterId);
    }
}
