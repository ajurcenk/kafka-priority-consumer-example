package com.redpanda;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class PriorityConsumerExample {

    private static final String CRITICAL_TOPIC = "critical-topic";
    private static final String HIGH_TOPIC = "high-topic";
    private static final String LOW_TOPIC = "low-topic";
    private static final int RATE_LIMIT = 100; // messages per second

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "redpanda-0.testzone.local:31092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "priority-consumer-group-v1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(CRITICAL_TOPIC, HIGH_TOPIC, LOW_TOPIC));

        long lastPollTime = System.currentTimeMillis();
        int processedMessages = 0;

        while (true) {

            long currentTime = System.currentTimeMillis();
            if (currentTime - lastPollTime >= 1000) {
                processedMessages = 0;
                lastPollTime = currentTime;
            }

            if (processedMessages < RATE_LIMIT) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                PriorityQueue<ConsumerRecord<String, String>> priorityQueue = new PriorityQueue<>(new Comparator<ConsumerRecord<String, String>>() {
                    @Override
                    public int compare(ConsumerRecord<String, String> o1, ConsumerRecord<String, String> o2) {
                        return getPriority(o1.topic()) - getPriority(o2.topic());
                    }
                });

                for (ConsumerRecord<String, String> record : records) {
                    priorityQueue.add(record);
                }

                while (!priorityQueue.isEmpty() && processedMessages < RATE_LIMIT) {
                    ConsumerRecord<String, String> record = priorityQueue.poll();
                    processMessage(record);
                    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset()+1, "no metadata"));
                    consumer.commitSync(currentOffsets);
                    processedMessages++;
                }
            } else {
                // Sleep for the remainder of the second to respect the rate limit
                try {
                    System.out.println("Sleep for the remainder of the second to respect the rate limit");
                    Thread.sleep(1000 - (System.currentTimeMillis() - lastPollTime));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

    }

    private static int getPriority(String topic) {
        return switch (topic) {
            case CRITICAL_TOPIC -> 1;
            case HIGH_TOPIC -> 2;
            case LOW_TOPIC -> 3;
            default -> Integer.MAX_VALUE;
        };
    }

    private static void processMessage(ConsumerRecord<String, String> record) {
        // Implement your message processing logic
        System.out.println("Processing message from topic: " + record.topic() + " with value: " + record.value());
    }
}
