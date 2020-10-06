package io.github.vikeshpandey;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class MSKConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MSKConsumer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final KafkaConsumer<String, String> consumer;

    public MSKConsumer(final Properties properties) {
        this.consumer = new KafkaConsumer<>(properties);
    }

    public void subscribeToTopic(final String topicName) {
        this.consumer.subscribe(Collections.singletonList(topicName));
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public void consumeMessage(String topicName) throws JsonProcessingException {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            if (records.count() == 0) {
                continue;
            }

            for (ConsumerRecord<String, String> record : records) {
                consumeMessageFromBroker(record);
            }
        }
    }

    private void consumeMessageFromBroker(ConsumerRecord<String, String> record) throws JsonProcessingException {
        JsonNode msg = OBJECT_MAPPER.readTree(record.value());
        LOGGER.info("Received message from broker, message is: " + msg.toString());
    }


}
