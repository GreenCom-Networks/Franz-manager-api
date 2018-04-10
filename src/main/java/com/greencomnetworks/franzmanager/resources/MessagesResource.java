package com.greencomnetworks.franzmanager.resources;

import com.greencomnetworks.franzmanager.entities.HttpError;
import com.greencomnetworks.franzmanager.entities.Message;
import com.greencomnetworks.franzmanager.services.AdminClientService;
import com.greencomnetworks.franzmanager.services.ConstantsService;
import com.greencomnetworks.franzmanager.utils.FUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Path("/topics/{topicId}/messages")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MessagesResource {
    private static final Logger logger = LoggerFactory.getLogger(MessagesResource.class);

    private final AdminClient adminClient;

    public MessagesResource() {
        this.adminClient = AdminClientService.getInstance();
    }

    public MessagesResource(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @GET
    public Object getMessages(@PathParam("topicId") String topicId,
                                     @DefaultValue("10") @QueryParam("quantity") Integer quantity) {
        KafkaFuture<Map<String, TopicDescription>> describedTopicsFuture = adminClient.describeTopics(Stream.of(topicId).collect(Collectors.toSet())).all();
        Map<String, TopicDescription> describedTopics = FUtils.getOrElse(() -> describedTopicsFuture.get(), null);
        if (describedTopics == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new HttpError(Response.Status.NOT_FOUND.getStatusCode(), "This topic (" + topicId + ") doesn't exist."))
                    .build();
        }

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConstantsService.brokersList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "franz-manager-api");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);


        List<TopicPartition> topicPartitions = consumer.partitionsFor(topicId).stream().map(partitionInfo -> new TopicPartition(topicId, partitionInfo.partition())).collect(Collectors.toList());

        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);

        for (TopicPartition topicPartition : topicPartitions) {
            long offset = consumer.position(topicPartition);
            long beginningOffset = beginningOffsets.get(topicPartition);

            if (offset - beginningOffset < quantity) {
                consumer.seekToBeginning(FUtils.List.of(topicPartition));
            } else {
                consumer.seek(topicPartition, offset - quantity);
            }
        }

        List<Message> messages = new ArrayList<>();
        final int giveUp = 5;
        int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
            } else {
                consumerRecords.forEach(record -> {
                    messages.add(new Message(record.value(), record.key(), record.partition(), record.offset(), record.timestamp()));
                });
                if (messages.size() == topicPartitions.size() * quantity) break;
            }
        }

        consumer.close();

        return messages.stream().sorted((m1, m2) -> -Long.compare(m1.timestamp, m2.timestamp)).limit(quantity).collect(Collectors.toList());
    }
}
