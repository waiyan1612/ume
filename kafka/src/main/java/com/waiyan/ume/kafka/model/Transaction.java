package com.waiyan.ume.kafka.model;

import java.time.LocalDateTime;

public class Transaction {
    private LocalDateTime purchasedDate;
    private int customerId;
    private String productId;

    // required by jackson
    public Transaction() {

    }

    public Transaction(LocalDateTime purchasedDate, int customerId, String productId) {
        this.purchasedDate = purchasedDate;
        this.customerId = customerId;
        this.productId = productId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public String getProductId() { return productId; }

    public LocalDateTime getPurchasedDate() {
        return purchasedDate;
    }

    @Override
    public String toString() {
        return "{" + this.purchasedDate + ": " + this.customerId + " bought " + this.productId + "}";
    }
}
