package com.greencomnetworks.franzmanager.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.greencomnetworks.franzmanager.entities.Cluster;
import com.greencomnetworks.franzmanager.entities.Message;
import com.greencomnetworks.franzmanager.services.AdminClientService;
import com.greencomnetworks.franzmanager.services.ConstantsService;
import com.greencomnetworks.franzmanager.utils.CustomObjectMapper;
import com.greencomnetworks.franzmanager.utils.KafkaUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.glassfish.grizzly.websockets.DataFrame;
import org.glassfish.grizzly.websockets.WebSocket;
import org.glassfish.grizzly.websockets.WebSocketApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import java.time.Duration;
import java.util.*;

public class LiveMessagesResource extends WebSocketApplication {
    private static final Logger logger = LoggerFactory.getLogger(LiveMessagesResource.class);

    private Map<WebSocket, FranzConsumer> franzConsumers = new HashMap<>();

    private static LiveMessagesResource instance = new LiveMessagesResource();

    public static LiveMessagesResource getInstance() {
        return instance;
    }

    @Override
    public void onMessage(WebSocket socket, String data) {
        if(StringUtils.isEmpty(data)) {
            logger.warn("Received empty message");
            return;
        }

        String[] actions = data.split(":");

        String action = actions[0];
        switch (action) {
            case "subscribe":
                if(actions.length < 3) {
                    logger.warn("Invalid subscribe action: '{}'", data);
                    return;
                }
                this.newSocketConsumer(socket, actions[1], actions[2]);
                break;
            case "close":
                socket.close();
                break;
            default:
                logger.warn("Unknown socket action : " + action);
                break;
        }
    }

    @Override
    public void onConnect(WebSocket socket) {
        logger.info("new websocket connection");
    }

    @Override
    public void onClose(WebSocket socket, DataFrame frame) {
        FranzConsumer franzConsumer = franzConsumers.get(socket);
        if(franzConsumer != null) {
            franzConsumer.shutdown();
            franzConsumers.remove(socket);
            logger.info("websocket closed, consumer " + franzConsumer.id + " closed.");
        } else {
            logger.info("websocket closed");
        }
    }

    private void newSocketConsumer(WebSocket socket, String topic, String clusterId) {
        AdminClient adminClient = AdminClientService.getAdminClient(clusterId);
        if(KafkaUtils.describeTopic(adminClient, topic) == null) {
            logger.warn("Trying to subscribe to an unknown topic: '{}' on '{}'", topic, clusterId);
            socket.close();
            return;
        }

        FranzConsumer franzConsumer = new FranzConsumer("franz-manager-api_live", topic, socket, clusterId);
        franzConsumers.put(socket, franzConsumer);
        franzConsumer.start();
    }

    private class FranzConsumer implements Runnable {

        private final KafkaConsumer<String, String> consumer;
        private final String id;
        private final String groupId;
        private final String topic;
        private final WebSocket socket;

        private final Thread thread;

        private FranzConsumer(String groupId,
                              String topic,
                              WebSocket socket,
                              String clusterId) {
            this.id = UUID.randomUUID().toString();
            this.groupId = groupId + '_' + id;
            this.topic = topic;
            this.socket = socket;
            Cluster cluster = null;
            for (Cluster c : ConstantsService.clusters) {
                if (StringUtils.equals(c.name, clusterId)) {
                    cluster = c;
                    break;
                }
            }
            if (cluster == null) {
                throw new NotFoundException("Cluster not found for the id " + clusterId);
            }

            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.brokersConnectString);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, this.groupId);
            Deserializer<String> deserializer = Serdes.String().deserializer();
            this.consumer = new KafkaConsumer<>(props, deserializer, deserializer);

            this.thread = new Thread(this);
        }

        @Override
        public void run() {
            try {
                ObjectMapper objectMapper = CustomObjectMapper.defaultInstance();
                List<TopicPartition> topicPartitions = KafkaUtils.topicPartitionsOf(consumer, this.topic);
                consumer.assign(topicPartitions);
                consumer.seekToEnd(topicPartitions);

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    List<Message> messages = new ArrayList<>();
                    for (ConsumerRecord<String, String> record : records) {
                        Message message = new Message(record.value(), record.key(), record.partition(), record.offset(), record.timestamp(), record.headers());
                        messages.add(message);
                    }
                    if (messages.size() > 0) {
                        logger.info("{}: consumed {} message(s)", this.id, String.valueOf(messages.size()));
                        try {
                            String s = objectMapper.writeValueAsString(messages);
                            this.socket.send(s);
                        } catch (JsonProcessingException e) {
                            logger.error("Unable to serializer messages: {}", e.getMessage(), e);
                        }
                        Thread.sleep(1000);
                    }
                }
            } catch (WakeupException | InterruptedException e) {
                // ignore for shutdown
            } finally {
                consumer.close();
            }
        }

        public void start() {
            thread.start();
        }

        public void shutdown() {
            consumer.wakeup();
            try { thread.join(2000); } catch (InterruptedException e) { /* noop */ }
        }
    }
}