package io.github.vikeshpandey;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

class MSKProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MSKProducer.class);
    private KafkaProducer<String, String> kafkaProducer;

    MSKProducer(final Properties properties) {
        this.kafkaProducer = new KafkaProducer<>(properties);

    }

    void sendMessage(final String topicName) {
        try {
            LOGGER.info("sending now");
            IntStream.range(1, 100).forEach(value -> sendMessageToBroker(topicName));
            LOGGER.info("sending done");
        } finally {
            LOGGER.info("sent all the messages shutting down the producer now");
            kafkaProducer.close();
        }
    }

    private void sendMessageToBroker(final String topicName) {
        long currentTimeMillis = System.currentTimeMillis();
        kafkaProducer.send(new ProducerRecord<>(topicName, "new message published at : " + currentTimeMillis));
        LOGGER.info("sent msg !!");

    }

}
