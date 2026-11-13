package com.kafka.kafkademo;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@RestController 
@RequestMapping ("/api/events")
public class EventProducerController {
    private final KafkaTemplate<String,String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic}")
    private String defaultTopic;

    public EventProducerController(KafkaTemplate<String,String> kafkaTemplate,ObjectMapper objectMapper){
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper=objectMapper;
    }
    @PostMapping ("/publish")
    public String publishFile(
        @RequestParam String filePath,
        @RequestParam(required = false) String topic) throws Exception
    {
        String targetTopic = (topic!=null) && !topic.isBlank() ? topic :defaultTopic;
        long start =System.currentTimeMillis();
        AtomicLong count=new AtomicLong(0);
        try(BufferedReader reader=Files.newBufferedReader(Path.of(filePath))){
            String line;
            while((line=reader.readLine())!=null){
                JsonNode node=objectMapper.readTree(line);
                String userId=node.get("user_id").asString();
                kafkaTemplate.send(targetTopic,userId,line);
                count.incrementAndGet();
            }
        }
        kafkaTemplate.flush();
        long elapsedTime=System.currentTimeMillis()-start;
        double rate=(count.get()*1000.0)/Math.max(1,elapsedTime);
        return String.format("Topic: %s | Published: %d events | Time: %d ms | Throughput: %.0f events/sec", targetTopic,count.get(),elapsedTime,rate);
    }
        @PostMapping("/generate")
    public ResponseEntity<String> generateAndPublish(
            @RequestParam(defaultValue = "1000000") int count,
            @RequestParam(required = false) String topic) {

        String targetTopic = (topic != null && !topic.isBlank()) ? topic : defaultTopic;
        String[] eventTypes = {"CLICK", "VIEW", "ADD_TO_CART", "PURCHASE", "LOGOUT"};
        String[] regions = {"us-west", "us-east", "eu-central", "ap-south"};
        java.util.concurrent.ThreadLocalRandom random = java.util.concurrent.ThreadLocalRandom.current();

        long start = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            String userId = "user-" + random.nextInt(10_000, 99_999);
            String json = String.format(
                "{\"event_id\":\"evt-%07d\",\"event_type\":\"%s\",\"user_id\":\"%s\",\"payload_value\":%.2f,\"timestamp\":\"2026-11-13T22:15:00\",\"region\":\"%s\"}",
                i,
                eventTypes[random.nextInt(eventTypes.length)],
                userId,
                random.nextDouble(5.0, 500.0),
                regions[random.nextInt(regions.length)]
            );
            kafkaTemplate.send(targetTopic, userId, json);
        }
        kafkaTemplate.flush();

        long elapsedMs = System.currentTimeMillis() - start;
        double rate = (count * 1000.0) / Math.max(1, elapsedMs);

        return ResponseEntity.ok(String.format("Topic: %s | Generated & Published: %d events | Time: %d ms | Throughput: %.0f events/sec",
                targetTopic, count, elapsedMs, rate));
    }

}
