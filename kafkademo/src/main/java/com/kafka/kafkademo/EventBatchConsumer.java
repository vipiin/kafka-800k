package com.kafka.kafkademo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Service;

import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class EventBatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventBatchConsumer.class);
    private final ObjectMapper objectMapper;
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong startTime = new AtomicLong(0);

    public EventBatchConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeBatch(List<String> records) {
        startTime.compareAndSet(0, System.currentTimeMillis());
        List<UserEvent> validBatch = new ArrayList<>(records.size());

        for (int i = 0; i < records.size(); i++) {
            try {
                UserEvent event = objectMapper.readValue(records.get(i), UserEvent.class);
                if (event.payloadValue() < 0) {
                    throw new IllegalArgumentException("Negative payload_value not allowed: " + event.eventId());
                }
                validBatch.add(event);
            } catch (Exception ex) {
                log.error("Poison pill detected at batch index {} -> {}", i, records.get(i));
                // Tells Spring Kafka to commit records 0..(i-1) and retry/DLT ONLY index i!
                throw new BatchListenerFailedException("Failed at index " + i, ex, i);
            }
        }

        long total = processedCount.addAndGet(validBatch.size());
        if (total % 200_000 == 0 || records.size() < 500) {
            long elapsed = System.currentTimeMillis() - startTime.get();
            double throughput = (total * 1000.0) / Math.max(1, elapsed);
            log.info("[THREAD: {}] Consumed: {} | Elapsed: {} ms | Throughput: {} events/sec",
                    Thread.currentThread().getName(), total, elapsed, String.format("%.0f", throughput));
        }
    }
}
