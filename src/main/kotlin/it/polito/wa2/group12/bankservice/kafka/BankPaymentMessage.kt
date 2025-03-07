package it.polito.wa2.group12.bankservice.kafka

import com.fasterxml.jackson.annotation.JsonProperty

data class BankPaymentMessage(
    @JsonProperty("transaction_id")
    val transaction_id: Long,
    @JsonProperty("status")
    val status: String,
    @JsonProperty("jwt")
    val jwt: String,
)
