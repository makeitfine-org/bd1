package com.frauddetection.flink.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.frauddetection.flink.model.FraudAlertEvent;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Flink serialization schema for FraudAlertEvent JSON messages to Kafka.
 */
public class FraudAlertSerializationSchema implements SerializationSchema<FraudAlertEvent> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = createObjectMapper();
    }

    @Override
    public byte[] serialize(FraudAlertEvent element) {
        try {
            if (objectMapper == null) {
                objectMapper = createObjectMapper();
            }
            return objectMapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize FraudAlertEvent", e);
        }
    }

    private ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
