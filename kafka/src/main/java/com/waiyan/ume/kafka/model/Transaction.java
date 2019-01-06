package com.waiyan.ume.kafka.model;

import java.time.LocalDate;

public class Transaction {
    private LocalDate purchasedDate;
    private int customerId;
    private String productId;

    // required by jackson
    public Transaction() {

    }

    public Transaction(LocalDate purchasedDate, int customerId, String productId) {
        this.purchasedDate = purchasedDate;
        this.customerId = customerId;
        this.productId = productId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public String getProductId() { return productId; }

    public LocalDate getPurchasedDate() {
        return purchasedDate;
    }

    @Override
    public String toString() {
        return "{" + this.purchasedDate + ": " + this.customerId + " bought " + this.productId + "}";
    }
}
