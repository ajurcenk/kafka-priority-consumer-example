package com.redpanda;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class TwoConsumersWithPauseResume {

    private static final String CRITICAL_TOPIC = "critical-topic";
    private static final String HIGH_TOPIC = "high-topic";
    private static final String LOW_TOPIC = "low-topic";

    private static final Lock lock = new ReentrantLock();
    private static final Condition condition = lock.newCondition();
    private static volatile boolean paused = false;

    public static void main(String[] args) throws Exception {

        Properties criticalConsumerConfig = new Properties();
        criticalConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "redpanda-0.testzone.local:31092");
        criticalConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "pause-resume-consumer-group-v1");
        criticalConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        criticalConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        criticalConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        criticalConsumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        final KafkaConsumer<String, String> criticalConsumer = new KafkaConsumer<>(criticalConsumerConfig);
        criticalConsumer.subscribe(Arrays.asList(CRITICAL_TOPIC));

        // Low priority consumer thread
        final Thread lowPriorityConsumeThread = new Thread(() -> {
            try {

                Properties lowPriorityConsumerConfig = new Properties();
                lowPriorityConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "redpanda-0.testzone.local:31092");
                lowPriorityConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "pause-resume-consumer-group-v1");
                lowPriorityConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                lowPriorityConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                lowPriorityConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                lowPriorityConsumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

                KafkaConsumer<String, String> lowPriorityConsumer = new KafkaConsumer<>(lowPriorityConsumerConfig);
                lowPriorityConsumer.subscribe(Arrays.asList(HIGH_TOPIC, LOW_TOPIC));

                // Pause the low priority consumer

                while (true) {
                    lock.lock();
                    try {
                        while (paused) {
                            condition.await();
                        }
                    } finally {
                        lock.unlock();
                    }
                    // Resume the low priority consumer if the low priority consumer is paused
                    ConsumerRecords<String, String> records = lowPriorityConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("lowPriorityConsumer: Processing data:" + record.value()
                                + " from topic, partition" + record.topic() + "," + record.partition());
                        lowPriorityConsumer.commitSync();
                    }
                } // while
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Star low priority consumer
        lowPriorityConsumeThread.start();

        // Wait for low priority thread `initialization
        Thread.sleep(7000);

        // Pause low priority consumer
        pauseThread();

        // Start critical priority consumer
        while (true) {

            ConsumerRecords<String, String> records = criticalConsumer.poll(Duration.ofMillis(100));

            // Any logic, to let the low priority thread consume data
            if (records.isEmpty()) {
                // Pause the high priority consumer
                // Resume low priority thread
                resumeThread();
                Thread.sleep(5000);
                // Pause low priority thread
                pauseThread();
                // Resume the high priority consumer
            }

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("criticalPriorityConsumer: Processing data:" + record.value()
                        + " from topic, partition" + record.topic() + "," + record.partition());
                criticalConsumer.commitSync();
            }
        }

    }

    public static void resumeThread() {
        lock.lock();
        try {
            paused = false;
            condition.signalAll();
            System.out.println("The low priority consumer thread resumed.");
        } finally {
            lock.unlock();
        }
    }

    public static void pauseThread() {
        lock.lock();
        try {
            paused = true;
            System.out.println("The low priority consumer paused.");
        } finally {
            lock.unlock();
        }
    }
}
