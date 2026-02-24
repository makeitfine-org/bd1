package com.frauddetection.transaction.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.transaction.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Service responsible for publishing transactions to the Kafka raw-transactions
 * topic.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic.raw-transactions}")
    private String rawTransactionsTopic;

    /**
     * Publishes a transaction event to Kafka.
     *
     * @param transaction the transaction to publish
     * @return a CompletableFuture with the send result
     */
    public CompletableFuture<SendResult<String, String>> publishTransaction(Transaction transaction) {
        try {
            String payload = objectMapper.writeValueAsString(transaction);
            log.info("Publishing transaction [{}] for user [{}] amount [{}] to topic [{}]",
                    transaction.getId(), transaction.getUserId(), transaction.getAmount(), rawTransactionsTopic);

            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(rawTransactionsTopic,
                    transaction.getUserId(), payload);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish transaction [{}]: {}", transaction.getId(), ex.getMessage(), ex);
                } else {
                    log.info("Transaction [{}] published to partition [{}] offset [{}]",
                            transaction.getId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });

            return future;
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize transaction [{}]", transaction.getId(), e);
            return CompletableFuture.failedFuture(e);
        }
    }
}
