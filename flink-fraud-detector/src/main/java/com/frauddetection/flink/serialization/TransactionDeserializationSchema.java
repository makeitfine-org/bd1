package com.frauddetection.flink.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.frauddetection.flink.model.TransactionEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Flink deserialization schema for TransactionEvent JSON messages from Kafka.
 */
public class TransactionDeserializationSchema implements DeserializationSchema<TransactionEvent> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = createObjectMapper();
    }

    @Override
    public TransactionEvent deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = createObjectMapper();
        }
        return objectMapper.readValue(message, TransactionEvent.class);
    }

    @Override
    public boolean isEndOfStream(TransactionEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TransactionEvent> getProducedType() {
        return TypeInformation.of(TransactionEvent.class);
    }

    private ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
