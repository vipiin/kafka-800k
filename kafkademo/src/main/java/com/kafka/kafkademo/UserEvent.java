package com.kafka.kafkademo;
import com.fasterxml.jackson.annotation.JsonProperty;

public record UserEvent(
    @JsonProperty("event_id") String eventId,
    @JsonProperty("event_type") String eventType,
    @JsonProperty("user_id") String userId,
    @JsonProperty("payload_value") double payloadValue,
    @JsonProperty("timestamp") String timestamp,
    @JsonProperty("region") String region
) {}
