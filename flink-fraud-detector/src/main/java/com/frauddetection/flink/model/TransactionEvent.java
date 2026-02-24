package com.frauddetection.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Represents a financial transaction event consumed from Kafka.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TransactionEvent implements Serializable {

    private String id;
    private String userId;
    private BigDecimal amount;
    private String merchantId;
    private String location;
    private String currency;
    private String category;
    private Instant timestamp;

    public TransactionEvent() {}

    public TransactionEvent(String id, String userId, BigDecimal amount, String merchantId,
                            String location, String currency, String category, Instant timestamp) {
        this.id = id;
        this.userId = userId;
        this.amount = amount;
        this.merchantId = merchantId;
        this.location = location;
        this.currency = currency;
        this.category = category;
        this.timestamp = timestamp;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "TransactionEvent{id='" + id + "', userId='" + userId + "', amount=" + amount +
                ", merchantId='" + merchantId + "', location='" + location + "'}";
    }
}
