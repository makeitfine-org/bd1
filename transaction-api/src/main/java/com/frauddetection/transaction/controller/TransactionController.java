package com.frauddetection.transaction.controller;

import com.frauddetection.transaction.model.Transaction;
import com.frauddetection.transaction.model.TransactionResponse;
import com.frauddetection.transaction.service.TransactionProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

/**
 * REST API controller for submitting financial transactions.
 */
@RestController
@RequestMapping("/api/transactions")
@Slf4j
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionProducerService producerService;

    /**
     * Submits a new financial transaction for fraud detection processing.
     *
     * @param transaction the transaction data
     * @return response with transaction ID and status
     */
    @PostMapping
    public ResponseEntity<TransactionResponse> submitTransaction(@RequestBody Transaction transaction) {
        // Assign ID and timestamp if not provided
        if (transaction.getId() == null || transaction.getId().isBlank()) {
            transaction.setId(UUID.randomUUID().toString());
        }
        if (transaction.getTimestamp() == null) {
            transaction.setTimestamp(Instant.now());
        }
        if (transaction.getCurrency() == null || transaction.getCurrency().isBlank()) {
            transaction.setCurrency("USD");
        }

        log.info("Received transaction submission: id={}, userId={}, amount={}",
                transaction.getId(), transaction.getUserId(), transaction.getAmount());

        producerService.publishTransaction(transaction);

        TransactionResponse response = TransactionResponse.builder()
                .transactionId(transaction.getId())
                .status("ACCEPTED")
                .message("Transaction submitted for fraud detection processing")
                .build();

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Transaction API is running");
    }
}
